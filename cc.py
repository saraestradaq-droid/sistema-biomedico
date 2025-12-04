import os
import json
import csv
import random
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import pymongo
from pymongo import MongoClient
from dataclasses import dataclass, asdict


@dataclass
class RegistroBiomedico:
    id: str
    fr: str  
    fc: str  
    spo2: str  


class Validador:
    @staticmethod
    def validar_id(id_str: str) -> bool:
        return bool(re.match(r'^ID-\d{3}$', id_str))
    
    @staticmethod
    def validar_fr(fr_str: str) -> bool:
        return bool(re.match(r'^\d{1,3}\s*Años$', fr_str))
    
    @staticmethod
    def validar_fc(fc_str: str) -> bool:
        return bool(re.match(r'^\d{2,3}ppm$', fc_str))
    
    @staticmethod
    def validar_spo2(spo2_str: str) -> bool:
        return bool(re.match(r'^\d{2,3}%$', spo2_str))
    
    @staticmethod
    def validar_registro(registro: RegistroBiomedico) -> bool:
        return all([
            Validador.validar_id(registro.id),
            Validador.validar_fr(registro.fr),
            Validador.validar_fc(registro.fc),
            Validador.validar_spo2(registro.spo2)
        ])

# ==================== GENERADOR DE DATOS ====================
class GeneradorDatos:
    @staticmethod
    def generar_registro(id_num: int) -> RegistroBiomedico:
        return RegistroBiomedico(
            id=f"ID-{id_num:03d}",
            fr=f"{random.randint(18, 90)} Años",
            fc=f"{random.randint(60, 120):03d}ppm",
            spo2=f"{random.randint(85, 100)}%"
        )
    
    @staticmethod
    def generar_registros(cantidad: int = 50) -> List[RegistroBiomedico]:
        return [GeneradorDatos.generar_registro(i+1) for i in range(cantidad)]


class Ordenador:
    @staticmethod
    def extraer_valor_fc(registro: RegistroBiomedico) -> int:
        return int(registro.fc.replace('ppm', ''))
    
    @staticmethod
    def ordenar_por_fc(registros: List[RegistroBiomedico]) -> List[RegistroBiomedico]:
        return sorted(registros, key=Ordenador.extraer_valor_fc)


class GestorArchivos:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.crear_estructura()
        self.configurar_log()
    
    def crear_estructura(self):
        carpetas = ['txt', 'csv', 'json']
        for carpeta in carpetas:
            (self.base_dir / carpeta).mkdir(parents=True, exist_ok=True)
    
    def configurar_log(self):
        logging.basicConfig(
            filename=self.base_dir / 'log.txt',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Sistema inicializado")
    
    def guardar_txt(self, registros: List[RegistroBiomedico], nombre: str):
        ruta = self.base_dir / 'txt' / f"{nombre}.txt"
        try:
            with open(ruta, 'w', encoding='utf-8') as f:
                for reg in registros:
                    f.write(f"{reg.id}, {reg.fr}, {reg.fc}, {reg.spo2}\n")
            logging.info(f"Archivo TXT guardado: {ruta}")
        except Exception as e:
            logging.error(f"Error guardando TXT: {e}")
    
    def guardar_csv(self, registros: List[RegistroBiomedico], nombre: str):
        ruta = self.base_dir / 'csv' / f"{nombre}.csv"
        try:
            with open(ruta, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['ID', 'Edad', 'Frecuencia Cardiaca', 'SpO2'])
                for reg in registros:
                    writer.writerow([reg.id, reg.fr, reg.fc, reg.spo2])
            logging.info(f"Archivo CSV guardado: {ruta}")
        except Exception as e:
            logging.error(f"Error guardando CSV: {e}")
    
    def guardar_json(self, registros: List[RegistroBiomedico], nombre: str):
        ruta = self.base_dir / 'json' / f"{nombre}.json"
        try:
            datos = [asdict(reg) for reg in registros]
            with open(ruta, 'w', encoding='utf-8') as f:
                json.dump(datos, f, indent=2, ensure_ascii=False)
            logging.info(f"Archivo JSON guardado: {ruta}")
        except Exception as e:
            logging.error(f"Error guardando JSON: {e}")
    
    def cargar_json(self, nombre: str) -> List[Dict]:
        ruta = self.base_dir / 'json' / f"{nombre}.json"
        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error cargando JSON: {e}")
            return []


class GestorMongoDB:
    def __init__(self, conexion_string: str, db_name: str = "biomedico_db"):
        try:
            self.client = MongoClient(conexion_string)
            self.db = self.client[db_name]
            self.coleccion = self.db["signos_vitales"]
            logging.info("Conexión a MongoDB establecida")
        except Exception as e:
            logging.error(f"Error conectando a MongoDB: {e}")
            raise
    
    def insertar_registros(self, registros: List[Dict]):
        try:
            if registros:
                self.coleccion.insert_many(registros)
                logging.info(f"Insertados {len(registros)} registros en MongoDB")
        except Exception as e:
            logging.error(f"Error insertando en MongoDB: {e}")
    
    def consultar_promedio_fc(self) -> float:
        try:
            pipeline = [
                {"$addFields": {
                    "fc_valor": {
                        "$toInt": {"$substrBytes": ["$fc", 0, {"$subtract": [{"$strLenBytes": "$fc"}, 3]}]}
                    }
                }},
                {"$group": {
                    "_id": None,
                    "promedio": {"$avg": "$fc_valor"}
                }}
            ]
            resultado = list(self.coleccion.aggregate(pipeline))
            return resultado[0]['promedio'] if resultado else 0
        except Exception as e:
            logging.error(f"Error consultando promedio FC: {e}")
            return 0
    
    def consultar_spo2_bajo(self, limite: int = 94) -> List[Dict]:
        try:
            pipeline = [
                {"$addFields": {
                    "spo2_valor": {
                        "$toInt": {"$substrBytes": ["$spo2", 0, {"$subtract": [{"$strLenBytes": "$spo2"}, 1]}]}
                    }
                }},
                {"$match": {"spo2_valor": {"$lt": limite}}},
                {"$project": {"spo2_valor": 0}}
            ]
            return list(self.coleccion.aggregate(pipeline))
        except Exception as e:
            logging.error(f"Error consultando SpO2 bajo: {e}")
            return []
    
    def exportar_consultas_json(self, gestor_archivos: GestorArchivos):
        try:
            consultas = {
                "promedio_fc": self.consultar_promedio_fc(),
                "spo2_bajo_94": self.consultar_spo2_bajo()
            }
            
            ruta = gestor_archivos.base_dir / 'json' / 'resultados.mongo.json'
            with open(ruta, 'w', encoding='utf-8') as f:
                json.dump(consultas, f, indent=2, ensure_ascii=False)
            logging.info(f"Consultas exportadas a JSON: {ruta}")
        except Exception as e:
            logging.error(f"Error exportando consultas: {e}")


class SistemaBiomedico:
    def __init__(self):
        self.gestor_archivos = GestorArchivos()
        self.validador = Validador()
        self.registros = []
        self.mongodb = None
    
    def inicializar_mongodb(self):
        conexion_string = input("Ingrese cadena de conexión MongoDB Atlas: ").strip()
        try:
            self.mongodb = GestorMongoDB(conexion_string)
            return True
        except:
            print("Error al conectar con MongoDB")
            return False
    
    def menu_principal(self):
        while True:
            print("\n" + "="*50)
            print("SISTEMA DE GESTIÓN BIOMÉDICA")
            print("="*50)
            print("1. Generar datos aleatorios")
            print("2. Validar datos")
            print("3. Ordenar por frecuencia cardíaca")
            print("4. Exportar datos (TXT, CSV, JSON)")
            print("5. Importar datos desde JSON")
            print("6. Conectar a MongoDB")
            print("7. Ejecutar consultas MongoDB")
            print("8. Mostrar registros")
            print("9. Salir")
            print("-"*50)
            
            opcion = input("Seleccione una opción: ")
            
            if opcion == "1":
                self.generar_datos()
            elif opcion == "2":
                self.validar_datos()
            elif opcion == "3":
                self.ordenar_datos()
            elif opcion == "4":
                self.exportar_datos()
            elif opcion == "5":
                self.importar_datos()
            elif opcion == "6":
                self.conectar_mongodb()
            elif opcion == "7":
                self.ejecutar_consultas()
            elif opcion == "8":
                self.mostrar_registros()
            elif opcion == "9":
                print("Saliendo del sistema...")
                break
            else:
                print("Opción inválida")
    
    def generar_datos(self):
        cantidad = input("Cantidad de registros a generar (default 50): ").strip()
        cantidad = int(cantidad) if cantidad.isdigit() else 50
        
        self.registros = GeneradorDatos.generar_registros(cantidad)
        print(f"Generados {len(self.registros)} registros")
    
    def validar_datos(self):
        if not self.registros:
            print("No hay registros para validar")
            return
        
        validos = [r for r in self.registros if self.validador.validar_registro(r)]
        invalidos = len(self.registros) - len(validos)
        
        print(f"Registros válidos: {len(validos)}")
        print(f"Registros inválidos: {invalidos}")
        
        if invalidos > 0:
            print("Registros inválidos detectados")
            self.registros = validos
    
    def ordenar_datos(self):
        if not self.registros:
            print("No hay registros para ordenar")
            return
        
        self.registros = Ordenador.ordenar_por_fc(self.registros)
        print("Registros ordenados por frecuencia cardíaca")
    
    def exportar_datos(self):
        if not self.registros:
            print("No hay registros para exportar")
            return
        
        nombre = input("Nombre base para archivos (sin extensión): ").strip()
        if not nombre:
            nombre = "registros_biomedicos"
        
        self.gestor_archivos.guardar_txt(self.registros, nombre)
        self.gestor_archivos.guardar_csv(self.registros, nombre)
        self.gestor_archivos.guardar_json(self.registros, nombre)
        print("Datos exportados en tres formatos")
    
    def importar_datos(self):
        nombre = input("Nombre del archivo JSON (sin extensión): ").strip()
        datos_json = self.gestor_archivos.cargar_json(nombre)
        
        if datos_json:
            self.registros = [RegistroBiomedico(**item) for item in datos_json]
            print(f"Importados {len(self.registros)} registros")
        else:
            print("No se pudieron importar los datos")
    
    def conectar_mongodb(self):
        if self.inicializar_mongodb():
            print("Conexión establecida con MongoDB Atlas")
    
    def ejecutar_consultas(self):
        if not self.mongodb:
            print("Primero debe conectar con MongoDB")
            return
        
        if self.registros:
            datos_dict = [asdict(r) for r in self.registros]
            self.mongodb.insertar_registros(datos_dict)
        
        promedio = self.mongodb.consultar_promedio_fc()
        spo2_bajos = self.mongodb.consultar_spo2_bajo()
        
        print(f"\nPromedio de frecuencia cardíaca: {promedio:.2f}ppm")
        print(f"Pacientes con SpO2 < 94%: {len(spo2_bajos)}")
        
        self.mongodb.exportar_consultas_json(self.gestor_archivos)
    
    def mostrar_registros(self, limite: int = 10):
        if not self.registros:
            print("No hay registros para mostrar")
            return
        
        print(f"\nMostrando {min(limite, len(self.registros))} de {len(self.registros)} registros:")
        print("-"*60)
        print(f"{'ID':<10} {'Edad':<12} {'FC':<10} {'SpO2':<8} {'Válido':<8}")
        print("-"*60)
        
        for reg in self.registros[:limite]:
            valido = "✓" if self.validador.validar_registro(reg) else "✗"
            print(f"{reg.id:<10} {reg.fr:<12} {reg.fc:<10} {reg.spo2:<8} {valido:<8}")


if __name__ == "__main__":
    try:
        sistema = SistemaBiomedico()
        sistema.menu_principal()
    except KeyboardInterrupt:
        print("\nPrograma interrumpido por el usuario")
    except Exception as e:
        print(f"Error inesperado: {e}")
        logging.error(f"Error inesperado: {e}")
