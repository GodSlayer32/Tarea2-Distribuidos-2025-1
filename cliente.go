package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "TAREA2-DISTRIBUIDOS-2025-1/cliente/proto"

	"google.golang.org/grpc"
)

type Emergencia struct {
	Name      string `json:"name"`
	Latitude  int32  `json:"latitude"`
	Longitude int32  `json:"longitude"`
	Magnitude int32  `json:"magnitude"`
}

func cargarEmergencias(path string) ([]Emergencia, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var emergencias []Emergencia
	err = json.NewDecoder(file).Decode(&emergencias)
	if err != nil {
		return nil, err
	}
	return emergencias, nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Uso: %s archivo_emergencias.json", os.Args[0])
	}
	archivoJSON := os.Args[1]

	emergencias, err := cargarEmergencias(archivoJSON)
	if err != nil {
		log.Fatalf("Error cargando emergencias: %v", err)
	}

	// Conectarse al servicio de asignación
	asignacionConn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("No se pudo conectar a asignación: %v", err)
	}
	defer asignacionConn.Close()
	asignacionClient := pb.NewAsignacionClient(asignacionConn)

	// Conectarse al servicio de monitoreo
	monitoreoConn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("No se pudo conectar a monitoreo: %v", err)
	}
	defer monitoreoConn.Close()
	monitoreoClient := pb.NewMonitoreoClient(monitoreoConn)

	fmt.Println("Emergencias recibidas")

	for _, e := range emergencias {
		fmt.Printf("\nEmergencia actual: %s magnitud %d en x = %d, y = %d\n", e.Name, e.Magnitude, e.Latitude, e.Longitude)

		req := &pb.EmergenciaRequest{
			Name:      e.Name,
			Latitude:  e.Latitude,
			Longitude: e.Longitude,
			Magnitude: e.Magnitude,
		}

		// Enviar al servicio de asignación
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := asignacionClient.EnviarEmergencia(ctx, req)
		cancel()
		if err != nil {
			log.Printf("Error al enviar emergencia: %v", err)
			continue
		}

		// Escuchar actualizaciones desde monitoreo
		stream, err := monitoreoClient.Actualizaciones(context.Background(), req)
		if err != nil {
			log.Printf("Error al iniciar stream de monitoreo: %v", err)
			continue
		}

		for {
			update, err := stream.Recv()
			if err == io.EOF {
				break // Stream finalizado
			}
			if err != nil {
				log.Printf("Error recibiendo actualización: %v", err)
				break
			}

			fmt.Printf("Emergencia: %s - Estado: %s - Dron: %s\n", update.GetName(), update.GetStatus(), update.GetDronId())

			if update.GetStatus() == "Extinguido" {
				fmt.Printf("Incendio %s ha sido extinguido por %s\n", update.GetName(), update.GetDronId())
				break
			}
		}
	}
}
