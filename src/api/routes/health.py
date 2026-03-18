from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def health():
    return {"status": "ok"}


@router.get("/readiness")
async def readiness():
    # Subfase 8.2: validar conexión MinIO e Iceberg aquí
    return {"status": "ready"}
