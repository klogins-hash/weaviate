# Railway Deployment Guide for Weaviate

This guide provides instructions for deploying Weaviate on Railway with optimized configuration.

## Quick Deploy

### Option 1: Deploy with Railway Button
[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/weaviate)

### Option 2: Manual Deployment

1. **Fork or clone this repository**
2. **Connect to Railway**:
   ```bash
   railway login
   railway link
   ```

3. **Deploy**:
   ```bash
   railway up
   ```

## Configuration Files

### `Dockerfile.railway`
Optimized multi-stage Docker build for Railway:
- Smaller image size (~50MB final image)
- Non-root user for security
- Health checks included
- Automatic PORT binding

### `railway.json` / `railway.toml`
Railway-specific configuration:
- Build settings
- Deploy commands
- Health check configuration
- Environment variables
- Volume mounts for persistence

### `.env.railway`
Environment configuration template with:
- Authentication settings
- Performance tuning
- Module configuration
- Backup settings

## Environment Variables

### Essential Variables
- `PORT`: Automatically set by Railway
- `PERSISTENCE_DATA_PATH`: Data storage path (default: `/var/lib/weaviate`)
- `AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED`: Allow anonymous access (default: `true`)

### Performance Tuning
- `GOMAXPROCS`: Number of CPU cores (default: `2`)
- `GOMEMLIMIT`: Memory limit (default: `1GiB`)
- Adjust based on your Railway plan

### Vectorizer Modules
```bash
# No vectorizer (default)
DEFAULT_VECTORIZER_MODULE=none

# OpenAI vectorizer
DEFAULT_VECTORIZER_MODULE=text2vec-openai
OPENAI_APIKEY=your-api-key

# Transformers vectorizer
DEFAULT_VECTORIZER_MODULE=text2vec-transformers
TRANSFORMERS_INFERENCE_API=http://your-transformer-service
```

## Deployment Steps

### 1. Initial Setup
```bash
# Clone the repository
git clone https://github.com/weaviate/weaviate.git
cd weaviate

# Set up Railway
railway login
railway init
```

### 2. Configure Environment
```bash
# Copy environment template
cp .env.railway .env

# Edit with your settings
nano .env

# Set variables in Railway
railway variables set AUTHENTICATION_APIKEY_ENABLED=true
railway variables set AUTHENTICATION_APIKEY_ALLOWED_KEYS=your-secure-key
```

### 3. Deploy
```bash
# Deploy using Railway CLI
railway up

# Or push to GitHub and auto-deploy
git add .
git commit -m "Configure for Railway deployment"
git push origin main
```

### 4. Add Persistent Volume
In Railway dashboard:
1. Go to your service settings
2. Add a volume mount at `/var/lib/weaviate`
3. Redeploy the service

## Monitoring

### Health Check
```bash
curl https://your-app.railway.app/v1/.well-known/ready
```

### Live Logs
```bash
railway logs
```

### Metrics Endpoint
```bash
curl https://your-app.railway.app/v1/.well-known/live
```

## Scaling

### Vertical Scaling
Upgrade your Railway plan for more resources:
- Adjust `GOMAXPROCS` and `GOMEMLIMIT` accordingly
- Restart the service

### Horizontal Scaling (Future)
For multi-node setup:
1. Enable cluster mode in environment variables
2. Deploy multiple services
3. Configure service discovery

## Backup Strategy

### Filesystem Backup
```bash
BACKUP_FILESYSTEM_PATH=/var/lib/weaviate/backups
```

### S3 Backup
```bash
BACKUP_S3_BUCKET=your-bucket
BACKUP_S3_ENDPOINT=s3.amazonaws.com
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

## Troubleshooting

### Container Won't Start
- Check logs: `railway logs`
- Verify PORT binding: Should use `$PORT` environment variable
- Check memory limits

### Performance Issues
- Increase `GOMAXPROCS` and `GOMEMLIMIT`
- Enable query result caching
- Optimize vector dimensions

### Data Persistence
- Ensure volume is mounted at `/var/lib/weaviate`
- Regular backups recommended

## Security Best Practices

1. **Enable Authentication**:
   ```bash
   AUTHENTICATION_APIKEY_ENABLED=true
   AUTHENTICATION_APIKEY_ALLOWED_KEYS=secure-random-key
   ```

2. **Use HTTPS**: Railway provides automatic SSL

3. **Limit Access**:
   ```bash
   AUTHORIZATION_ADMINLIST_ENABLED=true
   AUTHORIZATION_ADMINLIST_USERS=admin@example.com
   ```

4. **Regular Updates**: Keep Weaviate version current

## Support

- [Weaviate Documentation](https://weaviate.io/developers/weaviate)
- [Railway Documentation](https://docs.railway.app)
- [Weaviate Slack Community](https://weaviate.io/slack)
