"""Same-origin API and UI. No Hadoop, Spark, or network dependencies at runtime."""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from ml.predict_draft import predict
from web.snapshot import load_snapshot

STATIC = Path(__file__).resolve().parent/'static'


class DraftRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    radiant_heroes: list[StrictInt] = Field(min_length=5, max_length=5)
    dire_heroes: list[StrictInt] = Field(min_length=5, max_length=5)


def create_app(snapshot_path):
    snapshot_path = Path(snapshot_path).resolve()

    @asynccontextmanager
    async def lifespan(app):
        app.state.snapshot = load_snapshot(snapshot_path)
        yield
        del app.state.snapshot

    app = FastAPI(title='Dota 2 · Phân tích đội hình', lifespan=lifespan)
    app.mount('/hero-images', StaticFiles(directory=snapshot_path/'images', check_dir=False), name='hero-images')
    if STATIC.is_dir():
        app.mount('/static', StaticFiles(directory=STATIC), name='static')

    @app.get('/', include_in_schema=False)
    def index():
        return FileResponse(STATIC/'index.html')

    @app.get('/health')
    def health():
        manifest = app.state.snapshot['manifest']
        return {'status': 'ready', **{key: manifest[key] for key in ['snapshot_id', 'gold_run_id', 'model_run_id']}}

    @app.get('/api/heroes')
    def heroes():
        return [{key: hero[key] for key in ['id', 'name', 'image_url', 'supported']}
                for hero in app.state.snapshot['heroes']]

    @app.get('/api/dashboard')
    def dashboard():
        data = app.state.snapshot
        return {**data['dashboard'], 'model_metrics': data['metrics'],
                'snapshot': {key: data['manifest'][key] for key in ['snapshot_id', 'gold_run_id', 'model_run_id']}}

    @app.post('/api/predict')
    def prediction(draft: DraftRequest):
        try:
            return predict(app.state.snapshot['bundle'], draft.radiant_heroes, draft.dire_heroes)
        except (ValueError, OverflowError) as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    return app
