\# OrionflowETL



> A simple, extensible and schema‑first ETL library for .NET



\*\*OrionflowETL\*\* is an open‑source ETL (Extract, Transform, Load) library designed to make data pipelines \*\*explicit, composable and easy to reason about\*\*.



Although it was originally inspired by the needs of the Orionflow ecosystem, \*\*OrionflowETL is a fully independent project\*\* and can be used on its own in any .NET application.



Its main goal is to provide a \*\*lightweight execution engine\*\* for data pipelines where the \*structure and intent of the data are known upfront\*, avoiding hidden magic, heavy configuration or overly complex abstractions.



---



\## ✨ Why OrionflowETL?



Most ETL frameworks are either:



\* too heavyweight for simple data workflows, or

\* too opinionated and difficult to extend programmatically.



OrionflowETL takes a different approach:



\* \*\*Schema‑first\*\*: the structure of your data is explicit

\* \*\*Deterministic\*\*: no implicit discovery, no hidden inference

\* \*\*Extensible\*\*: new sources and sinks are added as plugins

\* \*\*Developer‑friendly\*\*: simple APIs, readable pipelines

\* \*\*Open source\*\*: built to evolve with community contributions



---



\## 🚀 Quick example



```csharp

var pipeline = Pipeline

&nbsp;   .FromCsv("products.csv")

&nbsp;   .WithSchema(schema)

&nbsp;   .Select(row => new

&nbsp;   {

&nbsp;       Id = row.GetInt("Id"),

&nbsp;       Name = row.GetString("Name")

&nbsp;   })

&nbsp;   .Collect();



pipeline.Execute();

```



The same pipeline can be created from other data sources without changing how you think about transformations:



```csharp

Pipeline.FromSqlServer(connString, "dbo.Products");

Pipeline.FromMySql(connString, "products");

Pipeline.FromPostgreSql(connString, "public.products");

Pipeline.FromMongoDb(connString, "products");

```



---



\## 🧠 Core concepts



OrionflowETL is built around a few simple ideas:



\### Row



A row is a typed, schema‑aware container of values.



\### Source



A source produces rows (CSV, relational databases, NoSQL, etc.).



\### Pipeline steps



Steps transform rows sequentially in a predictable way.



\### Sink



A sink consumes rows (in‑memory collection, database, file, etc.).



\### Executor



The executor runs the pipeline from start to end — nothing more, nothing less.



---



\## 🔌 Supported sources (roadmap)



\* ✅ CSV

\* 🔜 SQL Server

\* 🔜 MySQL

\* 🔜 PostgreSQL

\* 🔜 MongoDB



Each source lives in its own package and can evolve independently.



---



\## 🧩 Architecture philosophy



OrionflowETL follows a simple rule:



> \*\*Smart plans, simple runtime.\*\*



The library focuses on execution, not decision‑making. There is no implicit discovery, no runtime inference and no hidden state.



This makes pipelines:



\* easier to test

\* easier to debug

\* easier to extend



---



\## 🧪 Testing



The project encourages testing at multiple levels:



\* \*\*Unit tests\*\* for row access and type conversion

\* \*\*Integration tests\*\* for sources and sinks

\* \*\*Contract tests\*\* for compatibility between components



---



\## 🌍 Open‑source \& community



OrionflowETL is built with community growth in mind:



\* Modular architecture

\* Clear contribution boundaries

\* Friendly issues for first‑time contributors

\* Focus on clarity over cleverness



Contributions, ideas and discussions are welcome.



---



\## 📦 Project structure



```text

orionflow-etl/

&nbsp;├── src/

&nbsp;│   ├── OrionflowETL.Core

&nbsp;│   ├── OrionflowETL.Csv

&nbsp;│   ├── OrionflowETL.SqlServer

&nbsp;│   ├── OrionflowETL.MySql

&nbsp;│   ├── OrionflowETL.PostgreSql

&nbsp;│   ├── OrionflowETL.MongoDb

&nbsp;│   └── OrionflowETL.Tests

&nbsp;├── README.md

&nbsp;├── CONTRIBUTING.md

&nbsp;└── LICENSE

```



---



\## 🧭 Project status



OrionflowETL is currently in \*\*early development\*\*.



The API is intentionally small and focused. Breaking changes may occur until the first stable release.



---



\## 📄 License



This project is released under a permissive open‑source license (MIT or Apache 2.0).



---



\## ⭐ Join the project



If you enjoy building clean, understandable data systems:



\* ⭐ Star the repository

\* 🐛 Report issues

\* 🔧 Submit pull requests

\* 💬 Share ideas



OrionflowETL aims to grow with its users — thoughtfully and transparently.



