Projecto X

REFACTOR POINTS:

BUILD ASYNC FUCTIONS

STRUCTURE PROJECT:

## --> --> BACKEND <-- <--

--> MODULE - CONTROLER

---

## -> Controler modules

--> MODULE - START REQUERIMENTS

---

-> Set up settings
-> Create database

---

--> MODULE - GET DATA

---

-> Get historical data with API
-> Get realtime data with WEBSOCKET

---

--> MODULE - NORMALIZE GET DATA

---

## -> Normalize data from API/WEBSOCKET

--> MODULE - INSERT DATA IN DATABASE

---

-> Create structure data in DATABASE
-> Insert normalize data to DATABASE

---

--> MODULE - CHECK DATABASE INTEGRITY

---

-> Check that data is not missing and is in order
-> Recover missing data

---

--> MODULE - INDICATORS

---

## -> Calculate indicators -> Module Insert Data

--> MODULE - API

---

-> Create API for get info from DATABASE

## --> --> FRONT END <-- <--

--> MODULE - VIEW DATA

---

-> Create views of data

Factory Pattern: Para la creación de objetos de conexión a diferentes APIs de exchanges.
Strategy Pattern: Para normalizar datos de diferentes fuentes. Esto permitirá cambiar fácilmente la lógica de normalización sin afectar el resto del sistema.
Singleton Pattern: Para la conexión a la base de datos, asegurando que solo haya una instancia manejando las operaciones de la base de datos.
Observer Pattern: Para la recolección de datos en tiempo real, notificando a diferentes partes del sistema cuando hay nuevos datos disponibles.
Command Pattern: Para las operaciones de inserción y verificación de datos en la base de datos.

Para el controlador principal de tu aplicación, te recomendaría utilizar el Patrón de Diseño Facade. Este patrón proporciona una interfaz simplificada para un conjunto de interfaces en un subsistema, lo que facilita el uso del sistema sin necesidad de entender su complejidad interna.

Implementación del Facade Pattern
