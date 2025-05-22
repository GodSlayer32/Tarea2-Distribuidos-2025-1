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
	pb.UnimplementedDronesServer
	mongoClient *mongo.Client
	amqpChannel *amqp.Channel
}

func distancia(x1, y1, x2, y2 int32) float64 {
	return math.Sqrt(math.Pow(float64(x1-x2), 2) + math.Pow(float64(y1-y2), 2))
}

func (s *server) AsignarEmergencia(ctx context.Context, req *pb.AsignacionDronRequest) (*pb.EmergenciaResponse, error) {
	fmt.Printf("Dron %s asignado a %s\n", req.DronId, req.Name)

	// 1. Obtener posición actual del dron desde MongoDB
	drones := s.mongoClient.Database("emergenciasDB").Collection("drones")
	var dron struct {
		ID        string  `bson:"id"`
		Latitude  int32   `bson:"latitude"`
		Longitude int32   `bson:"longitude"`
		Status    string  `bson:"status"`
	}
	err := drones.FindOne(ctx, bson.M{"id": req.DronId}).Decode(&dron)
	if err != nil {
		log.Printf("No se encontró dron: %v", err)
		return nil, err
	}

	// 2. Simular desplazamiento (0.5s por unidad de distancia)
	dist := distancia(dron.Latitude, dron.Longitude, req.Latitude, req.Longitude)
	segundosMovimiento := time.Duration(dist*0.5) * time.Second
	for i := 0; i < int(segundosMovimiento.Seconds())/5; i++ {
		time.Sleep(5 * time.Second)
		s.publicarEstado(req, "En camino")
	}

	// 3. Simular apagado (2s por unidad de magnitud)
	for i := 0; i < int(req.Magnitude); i++ {
		time.Sleep(2 * time.Second)
		s.publicarEstado(req, "Apagando")
	}

	// 4. Publicar "Extinguido"
	s.publicarEstado(req, "Extinguido")

	// 5. Actualizar base de datos con nueva posición
	_, err = drones.UpdateOne(ctx, bson.M{"id": req.DronId}, bson.M{
		"$set": bson.M{
			"latitude": req.Latitude,
			"longitude": req.Longitude,
			"status": "available",
		},
	})
	if err != nil {
		log.Printf("Error actualizando dron: %v", err)
	}

	// 6. Enviar notificación a Registro
	regMsg := map[string]interface{}{
		"name":     req.Name,
		"status":   "Extinguido",
		"dron_id":  req.DronId,
	}
	regBody, _ := json.Marshal(regMsg)
	s.amqpChannel.Publish("", "registro_emergencias", false, false,
		amqp.Publishing{ContentType: "application/json", Body: regBody})

	// 7. Notificar a Asignacion
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err == nil {
		asignacionClient := pb.NewAsignacionClient(conn)
		asignacionClient.NotificarExtincion(ctx, &pb.EmergenciaRequest{
			Name:      req.Name,
			Latitude:  req.Latitude,
			Longitude: req.Longitude,
			Magnitude: req.Magnitude,
		})
		conn.Close()
	}

	return &pb.EmergenciaResponse{Message: "Emergencia atendida con éxito"}, nil
}

func (s *server) publicarEstado(req *pb.AsignacionDronRequest, status string) {
	msg := map[string]string{
		"name":    req.Name,
		"status":  status,
		"dron_id": req.DronId,
	}
	body, _ := json.Marshal(msg)
	err := s.amqpChannel.Publish(
		"", "estado_emergencias", false, false,
		amqp.Publishing{ContentType: "application/json", Body: body})
	if err != nil {
		log.Printf("Error publicando estado: %v", err)
	}
}

func main() {
	// Conexión a MongoDB
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("Error conectando a MongoDB: %v", err)
	}

	// Conexión a RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal RabbitMQ: %v", err)
	}

	_, _ = ch.QueueDeclare("estado_emergencias", false, false, false, false, nil)
	_, _ = ch.QueueDeclare("registro_emergencias", false, false, false, false, nil)

	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("Error iniciando puerto: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterDronesServer(s, &server{
		mongoClient: client,
		amqpChannel: ch,
	})

	fmt.Println("Servicio de drones corriendo en puerto 50053...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error en servidor gRPC: %v", err)
	}
}
