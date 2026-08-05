import { NestFactory } from '@nestjs/core';
import { ValidationPipe, Logger } from '@nestjs/common';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('Bootstrap');
  const app = await NestFactory.create(AppModule);

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

  SwaggerModule.setup('api', app, document, {
    customCssUrl:
      'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.min.css',
    customJs: [
      'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-bundle.min.js',
      'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-standalone-preset.min.js',
    ],
  });

  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`Server running on http://localhost:${port}`);
  logger.log(`Swagger UI documentation available at http://localhost:${port}/api`);
}

bootstrap();
