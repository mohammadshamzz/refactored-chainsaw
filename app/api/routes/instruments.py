
from fastapi import APIRouter, Request

router = APIRouter()

@router.get("/instruments")
def get_instruments(request: Request):
    return request.app.state.config['trading']['instruments']
