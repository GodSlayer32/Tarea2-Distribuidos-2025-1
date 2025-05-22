package main

import (

	"encoding/json"
	"fmt"
	"log"
	"net"

	pb "TAREA2-DISTRIBUIDOS-2025-1/cliente/proto"

	"github.com/streadway/amqp"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMonitoreoServer
}

// Mensaje desde RabbitMQ
type EstadoMensaje struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	DronId string `json:"dron_id"`
}

// Implementación del stream gRPC
func (s *server) Actualizaciones(req *pb.EmergenciaRequest, stream pb.Monitoreo_ActualizacionesServer) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return fmt.Errorf("error conectando a RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("error creando canal RabbitMQ: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"estado_emergencias", // nombre de la cola
		false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("error declarando la cola: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("error suscribiéndose a la cola: %v", err)
	}

	for d := range msgs {
		var estado EstadoMensaje
		err := json.Unmarshal(d.Body, &estado)
		if err != nil {
			log.Printf("error decodificando mensaje JSON: %v", err)
			continue
		}

		// Solo reenviar si el mensaje corresponde a la emergencia solicitada
		if estado.Name == req.GetName() {
			stream.Send(&pb.EstadoEmergencia{
				Name:   estado.Name,
				Status: estado.Status,
				DronId: estado.DronId,
			})

			if estado.Status == "Extinguido" {
				break
			}
		}
	}

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("falló el listener: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMonitoreoServer(s, &server{})

	fmt.Println("Servicio de monitoreo corriendo en puerto 50052...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("falló al iniciar gRPC: %v", err)
	}
}
