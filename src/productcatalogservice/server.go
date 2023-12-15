// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"math/rand"
	"sort"
	"strconv"
	"encoding/json"
	"io"

	amqp "github.com/rabbitmq/amqp091-go"


	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"cloud.google.com/go/profiler"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	cat          pb.ListProductsResponse
	catalogMutex *sync.Mutex
	log          *logrus.Logger
	extraLatency time.Duration

	port = "3550"

	reloadCatalog bool

//	rabConn *amqp.Connection
//	rabError error
)


type Product struct {
        Name string
        Sku int
        Price float32
        Category []string
        Manufacturer string
        Type string
}

func init() {
	log = logrus.New()
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.SetOutput(io.Discard)


	catalogMutex = &sync.Mutex{}
	err := readCatalogFile(&cat)
	if err != nil {
		log.Warnf("could not parse product catalog")
	}
}

func failOnError(err error, msg string) {
  if err != nil {
    log.Panicf("%s: %s", msg, err)
  }
}


func main() {
	if os.Getenv("ENABLE_TRACING") == "1" {
		err := initTracing()
		if err != nil {
			log.Warnf("warn: failed to start tracer: %+v", err)
		}
	} else {
		log.Info("Tracing disabled.")
	}

	if os.Getenv("DISABLE_PROFILER") == "" {
		log.Info("Profiling enabled.")
		go initProfiling("productcatalogservice", "1.0.0")
	} else {
		log.Info("Profiling disabled.")
	}

	flag.Parse()

	// set injected latency
	if s := os.Getenv("EXTRA_LATENCY"); s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("failed to parse EXTRA_LATENCY (%s) as time.Duration: %+v", v, err)
		}
		extraLatency = v
		log.Infof("extra latency enabled (duration: %v)", extraLatency)
	} else {
		extraLatency = time.Duration(0)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for {
			sig := <-sigs
			log.Printf("Received signal: %s", sig)
			if sig == syscall.SIGUSR1 {
				reloadCatalog = true
				log.Infof("Enable catalog reloading")
			} else {
				reloadCatalog = false
				log.Infof("Disable catalog reloading")
			}
		}
	}()



	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}
	log.Infof("starting grpc server at :%s", port)
	run(port)
	select {}
}

func run(port string) string {

	l, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}
	// Propagate trace context
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))
	var srv *grpc.Server
	srv = grpc.NewServer(
		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()))

	svc := &productCatalog{}

	pb.RegisterProductCatalogServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)
	serverError := srv.Serve(l)
	log.Fatal(serverError)
	return l.Addr().String()
}


func sendMsgToQueue(prod Product) error{
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()


	q, err := ch.QueueDeclare(
	  "hello", // name
	  false,   // durable
	  false,   // delete when unused
	  false,   // exclusive
	  false,   // no-wait
	  nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgStr, _ := json.Marshal(prod)

	body := msgStr

	err = ch.PublishWithContext(ctx,
	  "",     // exchange
	  q.Name, // routing key
	  false,  // mandatory
	  false,  // immediate
	  amqp.Publishing {
	    ContentType: "text/plain",
	    Body:        []byte(body),
	  })
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", body)

	return nil
}


func initStats() {
	// TODO(drewbr) Implement OpenTelemetry stats
}

func initTracing() error {
	var (
		collectorAddr string
		collectorConn *grpc.ClientConn
	)

	ctx := context.Background()

	mustMapEnv(&collectorAddr, "COLLECTOR_SERVICE_ADDR")
	mustConnGRPC(ctx, &collectorConn, collectorAddr)

	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithGRPCConn(collectorConn))
	if err != nil {
		log.Warnf("warn: Failed to create trace exporter: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	return err
}

func initProfiling(service, version string) {
	// TODO(ahmetb) this method is duplicated in other microservices using Go
	// since they are not sharing packages.
	for i := 1; i <= 3; i++ {
		if err := profiler.Start(profiler.Config{
			Service:        service,
			ServiceVersion: version,
			// ProjectID must be set if not running on GCP.
			// ProjectID: "my-project",
		}); err != nil {
			log.Warnf("failed to start profiler: %+v", err)
		} else {
			log.Info("started Stackdriver profiler")
			return
		}
		d := time.Second * 10 * time.Duration(i)
		log.Infof("sleeping %v to retry initializing Stackdriver profiler", d)
		time.Sleep(d)
	}
	log.Warn("could not initialize Stackdriver profiler after retrying, giving up")
}

type productCatalog struct{}

func readCatalogFile(catalog *pb.ListProductsResponse) error {
	catalogMutex.Lock()
	defer catalogMutex.Unlock()
	catalogJSON, err := ioutil.ReadFile("bestProducts.json")
	if err != nil {
		log.Fatalf("failed to open product catalog json file: %v", err)
		return err
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(catalogJSON), catalog); err != nil {
		log.Warnf("failed to parse the catalog JSON: %v", err)
		return err
	}


	sort.Slice(catalog.Products, func(i, j int) bool { 
		firstVal, _ := strconv.Atoi(catalog.Products[i].Id)
		secondVal, _ := strconv.Atoi(catalog.Products[j].Id)
		return firstVal < secondVal })

	isSorted := sort.SliceIsSorted(catalog.Products, func (i, j int) bool {
		firstVal, _ := strconv.Atoi(catalog.Products[i].Id)
                secondVal, _ := strconv.Atoi(catalog.Products[j].Id)
                return firstVal < secondVal })

	if (isSorted) {
		log.Info("Sorted Successfully")
	} else {
		log.Info("Sort did not work!!!!")
	}

	log.Info("successfully parsed product catalog json")
	return nil
}

func parseCatalog() []*pb.Product {
	if reloadCatalog || len(cat.Products) == 0 {
		err := readCatalogFile(&cat)
		if err != nil {
			return []*pb.Product{}
		}
	}
	return cat.Products
}


func insertProduct(index int, prod *pb.Product) error {
	products := parseCatalog()

	if (len(products) == index) {
		products = append(products, prod)
		return nil
	}

	products = append(products[:index+1], products[index:]...)
	products[index] = prod

	return nil


}

func (p *productCatalog) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (p *productCatalog) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}

func (p *productCatalog) ListProducts(context.Context, *pb.Empty) (*pb.ListProductsResponse, error) {
	time.Sleep(extraLatency)

	startNum := rand.Intn(1000)
	endNum := startNum + 10
	return &pb.ListProductsResponse{Products: parseCatalog()[startNum:endNum]}, nil
}

func (p *productCatalog) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	time.Sleep(extraLatency)
	var found *pb.Product

//	for i := 0; i < len(parseCatalog()); i++ {
//		if req.Id == parseCatalog()[i].Id {
//			found = parseCatalog()[i]
//		}
//	}

	i := sort.Search(len(parseCatalog()), func (ind int) bool {
		firstVal, _ := strconv.Atoi(parseCatalog()[ind].Id)
		secondVal, _ := strconv.Atoi(req.Id)
		return firstVal >= secondVal })

	if (i < len(parseCatalog()) && parseCatalog()[i].Id == req.Id) {

//		log.Info(fmt.Sprintf("Found at index: %d", i))
		found = parseCatalog()[i]
	} else {

//		log.Info("Product Not Found")
//		log.Info(fmt.Sprintf("index: %d", i))
	}


	if found == nil {
		return nil, status.Errorf(codes.NotFound, "no product with ID %s", req.Id)
	}
	return found, nil
}

func (p *productCatalog) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	time.Sleep(extraLatency)
	// Intepret query as a substring match in name or description.
	var ps []*pb.Product
	for _, p := range parseCatalog() {
		if strings.Contains(strings.ToLower(p.Name), strings.ToLower(req.Query)) ||
			strings.Contains(strings.ToLower(p.Description), strings.ToLower(req.Query)) {
			ps = append(ps, p)
		}
	}
	return &pb.SearchProductsResponse{Results: ps}, nil
}

func (p *productCatalog) AddNewProduct(ctx context.Context, req *pb.ProductNew) (*pb.Empty, error) {
	var found bool
	found = false

//	log.Info(fmt.Sprintf("+%v", req))

//	for i := 0; i < len(parseCatalog()); i++ {
//                if req.Id == parseCatalog()[i].Id {
//                        found = true
//			break
//                }
//        }


	i := sort.Search(len(parseCatalog()), func (ind int) bool {
                firstVal, _ := strconv.Atoi(parseCatalog()[ind].Id)
                secondVal, _ := strconv.Atoi(req.Id)
                return firstVal >= secondVal })

        if (i < len(parseCatalog()) && parseCatalog()[i].Id == req.Id) {

        	log.Info(fmt.Sprintf("Found at index: %d", i))
                found = true
        } else {

//              log.Info("Product Not Found")
//              log.Info(fmt.Sprintf("index: %d", i))
        }


        if found == true {
		log.Info("Product already exists")
                return nil, status.Errorf(codes.NotFound, "product with ID %s already exists!", req.Id)
        }

	freshProd := &pb.Product{Id: req.Id,
                        Name: req.Name,
                        Description: req.Description,
                        Picture: req.Picture,
                        PriceUsd: req.PriceUsd,
                        Categories: req.Categories}

	err := insertProduct(i, freshProd)


	if (err != nil) {
		log.Info("product added successfully")
        }

	strSku, _ := strconv.Atoi(req.Id)
	

	err = sendMsgToQueue(Product{
		Sku: strSku,
		Price: float32(req.PriceUsd.Units),
		Name: req.Name,
		Manufacturer: req.Manufacturer,
		Category: req.Categories,
		Type: req.Type})

	if (err != nil) {

		log.Info("Message failed to be send to RabbitMQ")
		return nil, err
	} else {

		log.Info("Message sent to RabbitMQ successfully")
	}

	log.Info(fmt.Sprintf("%+v", req))

	return &pb.Empty{}, nil
}

func mustMapEnv(target *string, envKey string) {
	v := os.Getenv(envKey)
	if v == "" {
		panic(fmt.Sprintf("environment variable %q not set", envKey))
	}
	*target = v
}

func mustConnGRPC(ctx context.Context, conn **grpc.ClientConn, addr string) {
	var err error
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	*conn, err = grpc.DialContext(ctx, addr,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	if err != nil {
		panic(errors.Wrapf(err, "grpc: failed to connect %s", addr))
	}
}
