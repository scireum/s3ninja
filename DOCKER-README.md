# S3Ninja - Configuration Docker Locale

## Démarrage rapide

### Prérequis
- Docker et Docker Compose installés
- Port 9000 disponible sur votre machine

### Lancement
1. **Méthode simple** : Double-cliquez sur `start-local.bat`
2. **Méthode manuelle** :
   ```cmd
   docker-compose up --build -d s3ninja
   ```

### Accès aux services
- **API S3** : http://localhost:9000
- **Interface Web** : http://localhost:9000/ui
- **Interface Web alternative** (optionnelle) : http://localhost:8080

### Arrêt
1. **Méthode simple** : Double-cliquez sur `stop-local.bat`
2. **Méthode manuelle** :
   ```cmd
   docker-compose down
   ```

## Configuration des ports

### Ports exposés
- **9000** : Port principal pour l'API S3 et l'interface web
- **8080** : Port alternatif pour l'interface web statique (optionnel)

### Personnalisation des ports
Pour changer les ports, modifiez la section `ports` dans `docker-compose.yml` :
```yaml
ports:
  - "VOTRE_PORT:9000"  # Remplacez VOTRE_PORT par le port souhaité
```

## Tests et développement

### Lancement avec client AWS CLI
```cmd
docker-compose --profile test up aws-cli
```

### Lancement avec interface web alternative
```cmd
docker-compose --profile ui up s3ninja-ui
```

### Configuration AWS CLI locale
```bash
aws configure set aws_access_key_id AKIAIOSFODNN7EXAMPLE
aws configure set aws_secret_access_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
aws configure set default.region us-east-1
aws configure set default.output json

# Test de connexion
aws --endpoint-url=http://localhost:9000 s3 ls
```

## Gestion des données

### Volumes
- **./data** : Stockage des données S3 (persistant)
- **./data/multipart** : Stockage des uploads multipart
- **s3ninja-logs** : Logs de l'application

### Sauvegarde/Restauration
Les données sont stockées dans le dossier `./data` et persistent entre les redémarrages.

Pour nettoyer toutes les données :
```cmd
docker-compose down -v
rmdir /s /q data
```

## Dépannage

### Vérifier l'état des services
```cmd
docker-compose ps
```

### Consulter les logs
```cmd
docker-compose logs -f s3ninja
```

### Rebuild complet
```cmd
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Test de connectivité
```cmd
curl http://localhost:9000/ui
```

## Intégration avec des applications

### Configuration pour applications Java (Spring Boot)
```properties
spring.cloud.aws.s3.endpoint=http://localhost:9000
spring.cloud.aws.credentials.access-key=AKIAIOSFODNN7EXAMPLE
spring.cloud.aws.credentials.secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
spring.cloud.aws.region.static=us-east-1
```

### Configuration pour applications Node.js
```javascript
const AWS = require('aws-sdk');

const s3 = new AWS.S3({
  endpoint: 'http://localhost:9000',
  accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
  secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
  region: 'us-east-1',
  s3ForcePathStyle: true
});
```
