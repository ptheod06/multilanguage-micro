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
	"context"
	"fmt"
	"net"
	"os"
	"time"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"math"
	"sort"
	"sync"
	"io"

	"cloud.google.com/go/profiler"
//	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	amqp "github.com/rabbitmq/amqp091-go"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/recommendationservice/genproto"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type ProdOut struct {

	Sku int
	Similarity float64
}

type SimProducts struct {

	Sku int
	SimilarProducts []ProdOut
}

type ProductComp struct {

	Item int
	Similarity float64
}

type Product struct {
	Name string
	Sku int
	Price float32
	Category []string
	Manufacturer string
	Type string
}



const (
	listenPort  = "8080"
)

var mu sync.RWMutex

var prods []SimProducts

var products []Product

var log *logrus.Logger


func failOnError(err error, msg string) {
  if err != nil {
    log.Panicf("%s: %s", msg, err)
  }
}

func init() {
	log = logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.SetOutput(io.Discard)
}

type recommendationService struct {
	productCatalogSvcAddr string
	productCatalogSvcConn *grpc.ClientConn
}

func runReceiver() {


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

	msgs, err := ch.Consume(
	  q.Name, // queue
	  "",     // consumer
	  true,   // auto-ack
	  false,  // exclusive
	  false,  // no-local
	  false,  // no-wait
	  nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
	  for d := range msgs {
	    var prodNew Product

	    json.Unmarshal(d.Body, &prodNew)
//	    log.Info(len(products))

	    products = append(products, prodNew)
//	    log.Info(products[len(products)-1])

	    calculateSimilarities(&prods)
	  }
	}()

	<-forever

}

func main() {
	ctx := context.Background()
	if os.Getenv("ENABLE_TRACING") == "1" {
		log.Info("Tracing enabled.")
		initTracing()

	} else {
		log.Info("Tracing disabled.")
	}

	if os.Getenv("DISABLE_PROFILER") == "" {
		log.Info("Profiling enabled.")
		go initProfiling("recommendationservice", "1.0.0")
	} else {
		log.Info("Profiling disabled.")
	}

	go runReceiver()

//	err := readRecommFile(&prods)
//	if err != nil {
//		log.Warnf("could not parse recommendations catalog")
//	}

	err_prods := readProducts(&products)
        if err_prods != nil {
                log.Warnf("could not parse products catalog")
        }

	calculateSimilarities(&prods)

	port := listenPort
	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}

	svc := new(recommendationService)
	mustMapEnv(&svc.productCatalogSvcAddr, "PRODUCT_CATALOG_SERVICE_ADDR")

	mustConnGRPC(ctx, &svc.productCatalogSvcConn, svc.productCatalogSvcAddr)

	log.Infof("service config: %+v", svc)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	var srv *grpc.Server

	// Propagate trace context always
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))
	srv = grpc.NewServer(
		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
	)

	pb.RegisterRecommendationServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)
	log.Infof("starting to listen on tcp: %q", lis.Addr().String())
	err = srv.Serve(lis)
	log.Fatal(err)
}

func initStats() {
	//TODO(arbrown) Implement OpenTelemetry stats
}

func initTracing() {
	var (
		collectorAddr string
		collectorConn *grpc.ClientConn
	)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

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

func readProducts(allProducts *[]Product) error {
	catalogJSON, err := ioutil.ReadFile("finaldetails_products.json")
        if err != nil {
                log.Fatalf("failed to open products catalog json file: %v", err)
                return err
        }
        if err := json.Unmarshal(catalogJSON, allProducts); err != nil {
                log.Warnf("failed to parse the catalog JSON: %v", err)
                return err
        }


        log.Info("successfully parsed products catalog json")
        return nil



}

func readRecommFile(catalog *[]SimProducts) error {
	catalogJSON, err := ioutil.ReadFile("recommProducts.json")
	if err != nil {
		log.Fatalf("failed to open recommendations catalog json file: %v", err)
		return err
	}
	if err := json.Unmarshal(catalogJSON, catalog); err != nil {
		log.Warnf("failed to parse the catalog JSON: %v", err)
		return err
	}
	log.Info("successfully parsed recommendations catalog json")
	return nil
}

func intersection(arr1, arr2 []string) []string {

	commons := []string{}

	for _, i := range arr1 {
		for _, j := range arr2 {
			if i==j {
				commons = append(commons, i)
			}

		}

	}
	return commons
}


func calculateSimilarities(allSimilarities *[]SimProducts) {

	var similarities [][]float64
	var topSimilarities [][]ProductComp


	log.Info("Starting to calculate Recommendations")

	before := time.Now()

	for i := 0; i < len(products); i++ {

		var inner_arr []float64


		for j:= 0; j < len(products); j++ {
			if i == j {
				inner_arr = append(inner_arr, -1.0)
				continue
			}

			numerator := len(intersection(products[i].Category, products[j].Category))

			denominator := math.Sqrt(float64(len(products[i].Category) + 3)) * math.Sqrt(float64(len(products[j].Category) + 3))

			if products[i].Price == products[j].Price {
				numerator += 1
			}

			if products[i].Type == products[j].Type {
                                numerator += 1
                        }

			if products[i].Manufacturer == products[j].Manufacturer { 
                                numerator += 1
                        }

			similarity := float64(numerator) / denominator
			inner_arr = append(inner_arr, similarity)
		}

		similarities = append(similarities, inner_arr)

	}


	for _, arrays := range similarities {

		var mostSimilar = []ProductComp{}

		for i := 0; i < 5; i++ {
			prod := ProductComp{i, arrays[i]}
			mostSimilar = append(mostSimilar, prod)
		}

		sort.Slice(mostSimilar, func(i, j int) bool { return mostSimilar[i].Similarity < mostSimilar[j].Similarity})
		for i := 5; i < len(arrays); i++ {
			if (mostSimilar[0].Similarity < arrays[i]) {
				prodNew := ProductComp{i, arrays[i]}
				mostSimilar[0] = prodNew
				sort.Slice(mostSimilar, func(i, j int) bool { return mostSimilar[i].Similarity < mostSimilar[j].Similarity })
			}

		}

		topSimilarities = append(topSimilarities, mostSimilar)
	}

	after := time.Since(before)

	log.Info("Finished recommendations. Done in ", after)

	var allNewSimilarities = []SimProducts{}


	for i := 0; i < len(products); i++ {
		var currSimilarities = []ProdOut{}

		for _, item := range topSimilarities[i] {

			currSimilarities = append(currSimilarities, ProdOut{products[item.Item].Sku, item.Similarity})
		}

		allNewSimilarities = append(allNewSimilarities, SimProducts{products[i].Sku, currSimilarities})

	}

	sort.Slice(allNewSimilarities, func (i, j int) bool {
		return allNewSimilarities[i].Sku < allNewSimilarities[j].Sku })


	mu.Lock()
	defer mu.Unlock()

	*allSimilarities = allNewSimilarities

}



func (rs *recommendationService) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (rs *recommendationService) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}


func (rs *recommendationService) ListRecommendations(ctx context.Context, in *pb.ListRecommendationsRequest) (*pb.ListRecommendationsResponse, error) {

	var similarProducts []string

	mu.RLock()
	defer mu.RUnlock()

//	for _, item := range prods {
//		if (in.ProductIds[0] == strconv.Itoa(item.Sku)) {
//			for i := 0; i < len(item.SimilarProducts); i++ {
//				similarProducts = append(similarProducts, strconv.Itoa(item.SimilarProducts[i].Sku))
//			}
//			break
//		}
//
//	}


	if (len(in.ProductIds) < 1) {
                return &pb.ListRecommendationsResponse{ProductIds: []string{"2099128"}}, nil
        }

	idStr, _ := strconv.Atoi(in.ProductIds[0])

	foundAt := sort.Search(len(prods), func (ind int) bool {
		return prods[ind].Sku >= idStr
	})

	if (foundAt < len(prods) && prods[foundAt].Sku == idStr) {
		for i := 0; i < len(prods[foundAt].SimilarProducts); i++ {
                	similarProducts = append(similarProducts, strconv.Itoa(prods[foundAt].SimilarProducts[i].Sku))
                }
	}


	if (len(similarProducts) < 4) {

		return &pb.ListRecommendationsResponse{ProductIds: []string{"2099128"}}, nil
	} else {

		return &pb.ListRecommendationsResponse{ProductIds: similarProducts[:4]}, nil
	}
}
