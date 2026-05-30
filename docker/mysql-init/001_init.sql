CREATE DATABASE IF NOT EXISTS `aieas`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE `aieas`;

SOURCE /docker-init/ddl.sql;
