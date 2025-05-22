package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"time"

	pb "TAREA2-DISTRIBUIDOS-2025-1/cliente/proto"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedAsignacionServer
	mongoClient *mongo.Client
	amqpChannel *amqp.Channel
}

type Dron struct {
	ID        string  `bson:"id"`
	Latitude  float64 `bson:"latitude"`
	Longitude float64 `bson:"longitude"`
	Status    string  `bson:"status"`
}

func distancia(lat1, lon1, lat2, lon2 float64) float64 {
	return math.Sqrt(math.Pow(lat1-lat2, 2) + math.Pow(lon1-lon2, 2))
}

func (s *server) EnviarEmergencia(ctx context.Context, req *pb.EmergenciaRequest) (*pb.EmergenciaResponse, error) {
	// 1. Buscar drones disponibles
	collection := s.mongoClient.Database("emergenciasDB").Collection("drones")
	cursor, err := collection.Find(ctx, bson.M{"status": "available"})
	if err != nil {
		return nil, fmt.Errorf("error buscando drones: %v", err)
	}
	defer cursor.Close(ctx)

	var dronSeleccionado Dron
	distMin := math.MaxFloat64
	encontrado := false

	for cursor.Next(ctx) {
		var dron Dron
		if err := cursor.Decode(&dron); err != nil {
			continue
		}

		dist := distancia(float64(req.Latitude), float64(req.Longitude), dron.Latitude, dron.Longitude)
		if dist < distMin {
			distMin = dist
			dronSeleccionado = dron
			encontrado = true
		}
	}

	if !encontrado {
		return &pb.EmergenciaResponse{Message: "No hay drones disponibles"}, nil
	}

	fmt.Printf("Se ha asignado %s a la emergencia %s\n", dronSeleccionado.ID, req.Name)

	// 2. Marcar dron como ocupado en MongoDB
	_, err = collection.UpdateOne(ctx, bson.M{"id": dronSeleccionado.ID}, bson.M{"$set": bson.M{"status": "busy"}})

	// 3. Notificar a los drones vía gRPC
	conn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("error conectando al servicio de drones: %v", err)
	}
	defer conn.Close()
	dronesClient := pb.NewDronesClient(conn)

	_, err = dronesClient.AsignarEmergencia(ctx, &pb.AsignacionDronRequest{
		Name:      req.Name,
		Latitude:  req.Latitude,
		Longitude: req.Longitude,
		Magnitude: req.Magnitude,
		DronId:    dronSeleccionado.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error al enviar asignación al dron: %v", err)
	}

	// 4. Enviar mensaje a RabbitMQ para el servicio de registro
	body, _ := json.Marshal(map[string]interface{}{
		"emergency_id": time.Now().UnixNano(), // ID único
		"name":         req.Name,
		"latitude":     req.Latitude,
		"longitude":    req.Longitude,
		"magnitude":    req.Magnitude,
		"status":       "En curso",
	})
	err = s.amqpChannel.Publish(
		"", "registro_emergencias", false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		log.Printf("Error enviando a RabbitMQ: %v", err)
	}

	return &pb.EmergenciaResponse{
		Message: fmt.Sprintf("Dron %s asignado a emergencia %s", dronSeleccionado.ID, req.Name),
	}, nil
}

func (s *server) NotificarExtincion(ctx context.Context, req *pb.EmergenciaRequest) (*pb.EmergenciaResponse, error) {
	// Restaurar el estado del dron
	drones := s.mongoClient.Database("emergenciasDB").Collection("drones")
	_, err := drones.UpdateOne(ctx, bson.M{"id": req.Name}, bson.M{"$set": bson.M{"status": "available"}})
	if err != nil {
		log.Printf("Error al actualizar estado de dron: %v", err)
	}

	return &pb.EmergenciaResponse{Message: "Emergencia marcada como extinguida"}, nil
}

func main() {
	// Conectar a MongoDB
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("Error conectando a MongoDB: %v", err)
	}

	// Conectar a RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal RabbitMQ: %v", err)
	}

	_, err = ch.QueueDeclare("registro_emergencias", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error declarando cola: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Error abriendo puerto: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAsignacionServer(grpcServer, &server{
		mongoClient: client,
		amqpChannel: ch,
	})

	fmt.Println("Servicio de asignación corriendo en puerto 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Fallo servidor gRPC: %v", err)
	}
}
