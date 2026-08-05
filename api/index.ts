import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from '../src/app.module';
import { ExpressAdapter } from '@nestjs/platform-express';
import express from 'express';

const server = express();
let isInitialized = false;

async function bootstrapServer() {
  if (!isInitialized) {
    const app = await NestFactory.create(
      AppModule,
      new ExpressAdapter(server),
    );

    app.useGlobalPipes(
      new ValidationPipe({
        transform: true,
        whitelist: true,
        forbidNonWhitelisted: true,
      }),
    );

    const config = new DocumentBuilder()
      .setTitle('BullMQ Nonce Transaction API')
      .setDescription(
        'Asynchronous blockchain transaction execution using BullMQ queue and atomic nonce management.',
      )
      .setVersion('1.0')
      .addTag('Transactions', 'Blockchain token transfer & queue monitoring endpoints')
      .build();

    const document = SwaggerModule.createDocument(app, config);
    SwaggerModule.setup('api', app, document);

    await app.init();
    isInitialized = true;
  }
}

export default async function handler(req: any, res: any) {
  await bootstrapServer();
  server(req, res);
}
