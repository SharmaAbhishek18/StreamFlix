# 🎬 StreamFlix

### Netflix-Style Distributed Video Streaming Platform

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x-brightgreen)
![Kafka](https://img.shields.io/badge/Apache-Kafka-black)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![Redis](https://img.shields.io/badge/Redis-Cache-red)
![Docker](https://img.shields.io/badge/Docker-Container-blue)
![FFmpeg](https://img.shields.io/badge/FFmpeg-Video-green)
![License](https://img.shields.io/badge/License-MIT-blue)

---

## 📖 Overview

**StreamFlix** is a scalable Netflix-inspired video streaming platform built using **Spring Boot Microservices**. It demonstrates an event-driven architecture where video uploads are processed asynchronously using **Apache Kafka**, encoded into adaptive **HLS** format using **FFmpeg**, stored in **AWS S3**, and streamed securely through **Pre-Signed URLs**.

The project is designed to showcase production-style backend development using modern cloud-native technologies such as Kafka, Redis, Docker, and AWS.

---

# ✨ Key Features

### Video Upload

* Upload videos through REST APIs
* Store original videos in AWS S3
* Generate unique video identifiers

### Event-Driven Processing

* Publish upload events to Kafka
* Asynchronous communication between services
* Loose coupling between microservices

### Video Encoding

* Automatic FFmpeg processing
* Adaptive HLS generation
* Multiple video resolutions
* Playlist (.m3u8) creation
* Segment (.ts) generation

### Streaming

* Secure Pre-Signed URLs
* Adaptive bitrate streaming
* Quality switching
* Fast playback startup
* Cached streaming URLs using Redis

### Custom Video Player

* Play / Pause
* Seek Bar
* Volume Control
* Playback Speed
* Subtitle Support
* Fullscreen
* Quality Selection
* Responsive UI

---

# 🏗️ Microservice Architecture

| Service               | Responsibility                                                |
| --------------------- | ------------------------------------------------------------- |
| **Content Service**   | Stores and manages movie metadata                             |
| **Video Service**     | Uploads original videos to AWS S3 and publishes Kafka events  |
| **Encoding Service**  | Downloads videos, performs FFmpeg encoding, uploads HLS files |
| **Streaming Service** | Generates secure streaming URLs and caches them in Redis      |

---

# 🔄 Complete Workflow

```text
Client
   │
   ▼
Upload Video
   │
   ▼
Content Service
   │
   ▼
Video Service
   │
Upload Original Video
to AWS S3
   │
   ▼
Publish VideoUploadedEvent
   │
   ▼
Apache Kafka
   │
   ▼
Encoding Service
   │
Download Original Video
   │
FFmpeg Encoding
   │
Generate HLS Files
   │
Upload HLS Files
to AWS S3
   │
Publish VideoEncodedEvent
   │
   ▼
Apache Kafka
   │
   ▼
Streaming Service
   │
Generate Pre-Signed URL
   │
Store URL in Redis
   │
   ▼
Custom HLS Player
   │
Adaptive Streaming
```

---

# ⚙️ Tech Stack

## Backend

* Java 21
* Spring Boot
* Spring MVC
* Spring Data JPA
* Hibernate
* Maven

## Database

* MySQL

## Cloud

* AWS S3

## Messaging

* Apache Kafka

## Caching

* Redis

## Video Processing

* FFmpeg

## Containerization

* Docker
* Docker Compose

## Testing

* Postman

---

# 📂 Repository Structure

```text
StreamFlix
│
├── content-service
├── video-service
├── encoding-service
├── streaming-service
├── player
├── docker-compose.yml
└── README.md
```

---

# 🚀 Service Responsibilities

## 📦 Content Service

* Store movie metadata
* Retrieve movie information
* Manage movie records

---

## 🎥 Video Service

Responsibilities:

* Accept multipart video uploads
* Upload original videos to AWS S3
* Publish upload event to Kafka

Produces:

```
video.uploaded
```

---

## ⚙️ Encoding Service

Responsibilities:

* Consume upload events
* Download original videos
* Generate adaptive HLS streams
* Create multiple resolutions
* Upload encoded files
* Publish completion event

Consumes:

```
video.uploaded
```

Produces:

```
video.encoded
```

---

## 📺 Streaming Service

Responsibilities:

* Consume encoding completion events
* Generate AWS Pre-Signed URLs
* Cache URLs in Redis
* Return streaming links

---

# 📡 REST APIs

## Content Service

| Method | Endpoint     | Description          |
| ------ | ------------ | -------------------- |
| GET    | /movies      | Retrieve all movies  |
| GET    | /movies/{id} | Retrieve movie by ID |
| POST   | /movies      | Add new movie        |

---

## Video Service

| Method | Endpoint                 |
| ------ | ------------------------ |
| POST   | /videos/upload/{movieId} |

---

## Streaming Service

| Method | Endpoint          |
| ------ | ----------------- |
| GET    | /stream/{movieId} |

---

# ⚡ Event Flow

```
Video Uploaded
      │
      ▼
video.uploaded
      │
      ▼
Encoding Service
      │
      ▼
Video Encoded
      │
      ▼
video.encoded
      │
      ▼
Streaming Service
```

---

# 🔒 Security

* AWS IAM Access
* Secure Object Storage
* Temporary Pre-Signed URLs
* Limited URL Expiration
* No Direct Bucket Access

---

# ⚡ Performance Optimizations

* Event-driven architecture
* Asynchronous processing
* Redis caching
* Stateless microservices
* Adaptive HLS streaming
* Reduced latency using cached URLs

---

# ▶️ Getting Started

## Clone Repository

```bash
git clone https://github.com/<your-username>/StreamFlix.git
```

---

## Start Infrastructure

```bash
docker compose up -d
```

This starts:

* Apache Kafka
* Zookeeper
* Redis

---

## Configure

Update the following in each service:

* Database configuration
* AWS credentials
* S3 bucket name
* Kafka bootstrap server
* Redis host

---

## Run Services

Start services in the following order:

1. Content Service
2. Video Service
3. Encoding Service
4. Streaming Service

---

## Upload Video

```
POST /videos/upload/{movieId}
```

---

## Stream Video

```
GET /stream/{movieId}
```

---

# 📷 Demo

Include screenshots of:

* Upload API
* Kafka Logs
* Encoding Logs
* AWS S3 Bucket
* Generated HLS Files
* Streaming API Response
* HTML Player
* Quality Selection
* Video Playback

---

# 🌟 Future Improvements

* JWT Authentication
* Spring Cloud Gateway
* Service Discovery
* Kubernetes Deployment
* GitHub Actions CI/CD
* Prometheus Monitoring
* Grafana Dashboards
* CDN Integration
* Video Analytics
* Resume Playback
* Watch History
* Recommendation Engine
* Search Service
* User Profiles

---

# 💡 Learning Outcomes

This project demonstrates practical experience with:

* Distributed Microservices
* Event-Driven Architecture
* Asynchronous Communication
* Cloud Storage
* Video Streaming
* HLS Protocol
* Redis Caching
* Dockerized Deployment
* RESTful API Design
* Production-Oriented Backend Development

---

# 👨‍💻 Author

**Abhishek**

Backend Developer

Java • Spring Boot • Microservices • Apache Kafka • AWS • Redis • Docker • FFmpeg

If you found this project useful, consider giving it a ⭐.
