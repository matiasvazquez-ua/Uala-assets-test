# Uala Asset Control (Google Sites Deploy)

## Ejecutar local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Configuración

Copiar `.env.example` a `.env` y completar variables.

En Streamlit Community Cloud, cargar las mismas variables en `Settings -> Secrets`.

## Deploy (Streamlit Community Cloud)

1. Subir este folder a un repo en GitHub.
2. En https://share.streamlit.io/ crear una app y elegir el repo + `app.py`.
3. En `Settings -> Secrets`, pegar el contenido de `.env` (sin comillas).
4. Verificar que la app abre correctamente.

## Embed en Google Sites

1. Abrir Google Sites.
2. Insertar -> `Embed` -> `By URL`.
3. Pegar la URL pública de Streamlit (ej: `https://miapp.streamlit.app`).
4. Ajustar el alto para que no quede scroll interno.

## Incluye

- Fixes de reglas de normalización y prioridad de dispatcher.
- Confirmación de acciones en chat sin rerun al detectar pending action.
- Config con fallback a `st.secrets`.
- Pantalla de setup si faltan credenciales Jira.
- Módulo de asignación automática con APScheduler y persistencia JSON local.
