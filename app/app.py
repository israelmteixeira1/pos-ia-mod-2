# ---------------------------------------------
# Classificador de Risco de Déficit Energético 
# ---------------------------------------------

import os
import json
import time
import numpy as np
import pandas as pd
import streamlit as st
import joblib
import altair as alt  

try:
    import shap
    HAS_SHAP = True
except Exception:
    HAS_SHAP = False

FEATURES_JSON = os.environ.get("FEATURES_JSON", "features.json")            
MODEL_PATH    = os.environ.get("MODEL_PATH", "modelo_RandomForest.joblib")  
DEMO_CSV      = os.environ.get("DEMO_CSV", "df_demo.csv")    


ORD_MAP   = {"Baixo": 0, "Medio": 1, "Alto": 2}
IDX2LABEL = {v: k for k, v in ORD_MAP.items()}


@st.cache_data
def load_features_list(path: str = FEATURES_JSON) -> list:
    with open(path, "r") as f:
        feats = json.load(f)
    return list(feats)

def align_and_sanitize(df: pd.DataFrame, feats: list) -> pd.DataFrame:
    X = df.copy()
    for c in feats:
        if c not in X.columns:
            X[c] = 0.0
    X = X[feats]
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype("float64")
    return X

@st.cache_resource
def load_model(path: str = MODEL_PATH):
    return joblib.load(path)

def local_inference(instances: list) -> dict:
    model = load_model()
    X = pd.DataFrame(instances)

    t0 = time.time()
    preds = model.predict(X)
    elapsed = time.time() - t0

    try:
        preds_int = [int(p) for p in preds]
    except Exception:
        preds_int = preds.tolist() if hasattr(preds, "tolist") else list(preds)

    proba = None
    try:
        if hasattr(model, "predict_proba"):
            proba_arr = model.predict_proba(X)
            if proba_arr.ndim == 2 and proba_arr.shape[1] == 3:
                proba = proba_arr[:, [ORD_MAP["Baixo"], ORD_MAP["Medio"], ORD_MAP["Alto"]]].tolist()
            else:
                proba = proba_arr.tolist()
    except Exception:
        proba = None

    labels = [IDX2LABEL.get(p, str(p)) for p in preds_int]
    return {"predictions": preds_int, "labels": labels, "proba": proba, "elapsed": elapsed}

def format_output(raw: dict) -> pd.DataFrame:
    preds  = raw.get("predictions", [])
    labels = raw.get("labels", [])
    proba  = raw.get("proba", None)
    out = pd.DataFrame({"label": labels, "pred": preds})
    if proba is not None:
        try:
            proba_df = pd.DataFrame(proba, columns=["p_Baixo", "p_Medio", "p_Alto"])
        except Exception:
            proba_df = pd.DataFrame(proba)
        out = pd.concat([out, proba_df], axis=1)
    return out

def risk_badge(label: str) -> str:
    return {"Baixo": "✅", "Medio": "🟨", "Alto": "🟥"}.get(label, "❔")

def playbook(p_b: float, p_m: float, p_a: float) -> str:
    if p_a >= 0.60:
        return "Acionar térmicas contratadas; revisar intercâmbio SE/CO; informar ONS/CCEE; plano de contingência de carga."
    if p_m >= 0.60:
        return "Reforçar monitoramento intradiário; reavaliar MRE/PLD; preparar despacho adicional; atenção a reservatórios."
    return "Operação normal. Manter monitoramento e atualização diária de dados."


st.set_page_config(page_title="Classificador de Risco Energético", layout="wide")
st.title("🔌 Classificação de Risco de Déficit Energético")

st.sidebar.markdown("### Configuração")
st.sidebar.write(f"**Arquivo do modelo:** `{MODEL_PATH}`")

model_loaded = False
try:
    _ = load_model()
    st.sidebar.success("Modelo carregado com sucesso.")
    model_loaded = True
except Exception as e:
    st.sidebar.error(
        "⚠️ Erro ao carregar o modelo.\n\n"
        f"{e}\n\n"
        "Dica: alinhe as versões `scikit-learn`/`numpy` do ambiente atual com as do treino "
        "ou re-salve o modelo neste ambiente."
    )

try:
    feats = load_features_list()
except Exception:
    feats = []


from datetime import datetime
import glob

def pick_csv_file() -> str | None:
    demo = "df_demo.csv"
    if os.path.exists(demo):
        return demo
    files = [f for f in glob.glob("*.csv") if os.path.isfile(f)]
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]

csv_path = pick_csv_file()
if csv_path is None:
    st.error("Não encontrei nenhum CSV na pasta do app (esperava `df_demo.csv` ou algum `*.csv`).")
    st.stop()

try:
    df_in = pd.read_csv(csv_path)
except Exception as e:
    st.error(f"Falha ao ler CSV: {e}")
    st.stop()

dt_col = None
for c in df_in.columns:
    if c.upper() in ("DATA_REFERENCIA", "DATA", "DATE"):
        dt_col = c
        break

if dt_col:
    df_in[dt_col] = pd.to_datetime(df_in[dt_col], errors="coerce")
    df_in = df_in.sort_values(dt_col).reset_index(drop=True)
    df_aug = df_in[df_in[dt_col].dt.month == 8].copy()
    target_row = df_aug.iloc[[-1]] if len(df_aug) > 0 else df_in.iloc[[-1]]
else:
    target_row = df_in.iloc[[-1]]

drop_cols = [c for c in ["NIVEL_RISCO_CATEGORIA", "SCORE_RISCO_NUMERICO", dt_col] if c and c in target_row.columns]
X_aligned = align_and_sanitize(target_row.drop(columns=drop_cols, errors="ignore"), feats)

if st.button("▶️ Prever com base em D-1", type="primary"):
    if not model_loaded:
        st.error("Modelo não carregado.")
    else:
        with st.spinner("Inferindo..."):
            raw = local_inference(X_aligned.to_dict(orient="records"))
        out = format_output(raw)

        display_out = out.copy()
        for c in ["p_Baixo", "p_Medio", "p_Alto", "pred"]:
            if c in display_out.columns:
                display_out.drop(columns=c, inplace=True)
        display_out.rename(columns={"label": "Risco Predito"}, inplace=True)
        if dt_col:
            display_out.insert(0, dt_col, target_row[dt_col].reset_index(drop=True))
        if "NOM_SUBSISTEMA" in target_row.columns:
            display_out.insert(1, "NOM_SUBSISTEMA", target_row["NOM_SUBSISTEMA"].reset_index(drop=True))

        st.success(f"OK em {raw.get('elapsed', 0):.2f}s — {len(display_out)} linha(s)")
        st.dataframe(display_out, use_container_width=True)

        label = str(out.loc[0, "label"])
        badge = risk_badge(label)
        st.markdown(f"### {badge} Risco **{label}**")

        pvals = out.loc[0, ["p_Baixo","p_Medio","p_Alto"]] if {"p_Baixo","p_Medio","p_Alto"}.issubset(out.columns) else None
        if pvals is not None:
            msg = playbook(float(pvals['p_Baixo']), float(pvals['p_Medio']), float(pvals['p_Alto']))
            st.info(f"**Sugestão operacional:** {msg}")

        if HAS_SHAP and len(df_in) > 0:
            model = load_model()
            ctx_global = df_in.tail(1000).copy()
            Xg = align_and_sanitize(
                ctx_global.drop(columns=[c for c in ["NIVEL_RISCO_CATEGORIA","SCORE_RISCO_NUMERICO", dt_col] if c in ctx_global], errors="ignore"),
                feats
            )
            try:
                explainer = shap.TreeExplainer(model)
            except Exception:
                explainer = shap.Explainer(model)
            sv = explainer(Xg)
            if isinstance(sv, list):
                arr = np.stack([s.values for s in sv], axis=-1)  # [n,f,C]
                mean_abs = np.abs(arr).mean(axis=(0,2))
            else:
                arr = sv.values
                if arr.ndim == 3:
                    mean_abs = np.abs(arr).mean(axis=(0,2))
                else:
                    mean_abs = np.abs(arr).mean(axis=0)
            k = min(10, Xg.shape[1])
            top_idx = np.argsort(mean_abs)[::-1][:k]  # do maior para o menor
            df_imp = pd.DataFrame({
                "feature": Xg.columns[top_idx],
                "importance": mean_abs[top_idx]
            }).sort_values("importance", ascending=False)

            chart = (
                alt.Chart(df_imp)
                .mark_bar()
                .encode(
                    x=alt.X("importance:Q", title="|SHAP| médio (global)"),
                    y=alt.Y("feature:N", sort="-x", title=None),  # ordena maior→menor
                    tooltip=[alt.Tooltip("feature:N", title="feature"),
                             alt.Tooltip("importance:Q", format=".4f", title="|SHAP|")]
                )
                .properties(height=max(200, 24 * k), width="container")
            )
            st.subheader("Principais fatores (Top-10 |SHAP| global)")
            st.altair_chart(chart, use_container_width=True)

        st.session_state["_last_X"] = X_aligned
        st.session_state["_last_out"] = out
