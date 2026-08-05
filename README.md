# BullMQ Nonce Transaction NestJS API

A production-ready NestJS application demonstrating **BullMQ queue handling**, **EVM Blockchain RPC integration**, and **Atomic Nonce Reservation** to eliminate transaction nonce conflicts.

---

## 🚀 Features

- **Asynchronous Execution**: `POST /transaction/send` enqueues jobs into BullMQ and responds immediately with `jobId`.
- **Atomic Nonce Reservation**: Prevents duplicate nonce assignment across concurrent transfer requests using database locks (`database.sqlite` / TypeORM).
- **Real Blockchain Integration**: Connects via `ethers.JsonRpcProvider` and signs/broadcasts transactions via `ethers.Wallet(PRIVATE_KEY)`.
- **Native & ERC-20 Support**: Supports both native chain currency transfers and ERC-20 smart contract token transfers (`tokenAddress`).
- **Retries & Monitoring**: 3-attempt exponential backoff retries and live queue metrics (`GET /queue/status`).
- **Interactive Swagger UI**: Interactive API documentation available at `/api`.

---

## 🛠️ Environment Setup (`.env`)

Create a `.env` file in the root directory:

```env
PORT=3000
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=secret
RPC_URL=https://testnetrpc.mstblockchain.com/
PRIVATE_KEY=your_private_key_here
```

---

## 📦 Local Installation & Running

```bash
# Install dependencies
npm install

# Run in development mode
npm run start:dev
```

Open **[http://localhost:3000/api](http://localhost:3000/api)** for Swagger UI.

---

## 🌐 Deployment Instructions

### Option 1: Deploy to Render (Recommended for BullMQ Persistent Workers)
1. Push this code to GitHub: `https://github.com/tanushree-excelr/queue-nest`.
2. Go to [Render.com](https://render.com) -> New **Redis** (Free Tier).
3. Go to Render.com -> New **Web Service** -> Connect `tanushree-excelr/queue-nest`.
4. Set Environment Variables on Render:
   - `REDIS_HOST`: (your Render Redis internal host)
   - `REDIS_PORT`: `6379`
   - `REDIS_PASSWORD`: (your Render Redis password)
   - `RPC_URL`: `https://testnetrpc.mstblockchain.com/`
   - `PRIVATE_KEY`: (your wallet private key)
5. Build Command: `npm run build` | Start Command: `npm run start:prod`.

---

### Option 2: Deploy to Vercel (Using Upstash Redis)
1. Create a free Serverless Redis database at [Upstash.com](https://upstash.com).
2. Push this repo to GitHub (`tanushree-excelr/queue-nest`).
3. Connect repo to [Vercel.com](https://vercel.com).
4. Add Environment Variables on Vercel:
   - `REDIS_HOST`: (Upstash endpoint host)
   - `REDIS_PORT`: `6379` (or `6379` / SSL port)
   - `REDIS_PASSWORD`: (Upstash password)
   - `RPC_URL`: `https://testnetrpc.mstblockchain.com/`
   - `PRIVATE_KEY`: (your wallet private key)
