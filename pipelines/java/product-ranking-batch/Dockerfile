# ------------------------------------
# 1. ESTÁGIO DE BUILD
# ------------------------------------
FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app

# O caminho AGORA é relativo à raiz do projeto (o contexto '.')
# Ex: ./pipelines/java/product-ranking-batch/pom.xml

COPY ./pipelines/java/product-ranking-batch/pom.xml .
RUN mvn dependency:go-offline

COPY ./pipelines/java/product-ranking-batch/src src
RUN mvn package -DskipTests

# ------------------------------------
# 2. ESTÁGIO DE RUNTIME
# ------------------------------------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
# ... (restante do runtime)