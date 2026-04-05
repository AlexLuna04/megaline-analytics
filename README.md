# Megaline Analytics

## Setup

1. Clonar el repo
git clone https://github.com/TU_USUARIO/megaline-analytics.git
cd megaline-analytics

2. Crear el .env con tus variables (hay un .env.example de guía)

3. Levantar los servicios
docker-compose up --build -d

4. Acceder a:
   - Mage AI:  http://localhost:6789
   - PgAdmin:  http://localhost:8085
   - Jupyter (Spark): http://localhost:8888

## Diagrama ERD
Ver [docs/erd.md](./docs/erd.md) para el modelo dimensional Gold.

## Comenatario
Tanto el data loader como la parte de dbt se pueden encontrar dentro de mage en [mageai-data/default_repo] (./mageai-data/default_repo), la parte de spark esta en [notebooks/avg_revenue_per_plan.ipynb] (./notebooks/avg_revenue_per_plan.ipynb) y también se incluyó la parte extra en su respectiva carpeta.