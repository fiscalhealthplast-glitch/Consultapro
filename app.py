import streamlit as st
import pandas as pd
import sqlite3
import requests
import time
import re
import plotly.express as px

# =========================================================
# CONFIGURAÇÃO DA PÁGINA
# =========================================================
st.set_page_config(
    page_title="ConsultaPro",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# BANCO DE DADOS (SQLite)
# =========================================================
DB = "consultas.db"

def init_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cnpj (
        cnpj TEXT PRIMARY KEY,
        nome TEXT,
        cidade TEXT,
        uf TEXT,
        cep TEXT,
        situacao TEXT,
        simples TEXT,
        endereco TEXT,
        ie TEXT,
        email TEXT,
        telefone TEXT,
        cnae_descricao TEXT,
        data_situacao TEXT
    )
    """)
    conn.commit()
    conn.close()

    # Cria tabela CEP
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cep (
        cep TEXT PRIMARY KEY,
        logradouro TEXT,
        bairro TEXT,
        cidade TEXT,
        uf TEXT
    )
    """)
    conn.commit()
    conn.close()

init_db()

# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def extrair_ie(cnpj, dados_api=None):
    """
    Tenta extrair a Inscrição Estadual a partir dos dados já obtidos ou fazendo nova consulta.
    """
    # Se recebeu dados da BrasilAPI ou CNPJ.ws, procura neles primeiro
    if dados_api:
        estab = dados_api.get("estabelecimento")
        if isinstance(estab, dict):
            # IE direta
            if estab.get("inscricao_estadual"):
                return estab.get("inscricao_estadual")
            # Lista de inscrições estaduais
            ies = estab.get("inscricoes_estaduais")
            if isinstance(ies, list):
                for item in ies:
                    ie = item.get("inscricao_estadual")
                    if ie:
                        return ie

    # Tenta consulta específica na CNPJ.ws (que geralmente tem a IE)
    try:
        r = requests.get(f"https://publica.cnpj.ws/cnpj/{cnpj}", timeout=10,
                         headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            d = r.json()
            estab = d.get("estabelecimento", {})
            ie = estab.get("inscricao_estadual")
            if ie:
                return ie
            ies = estab.get("inscricoes_estaduais", [])
            for item in ies:
                ie = item.get("inscricao_estadual")
                if ie:
                    return ie
    except:
        pass

    return ""  # Retorna vazio se não encontrou

def consultar_cnpj(cnpj):
    """Consulta um CNPJ nas APIs públicas e retorna um dicionário com todos os dados."""
    cnpj = re.sub(r'\D', '', cnpj)

    def get(url):
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            return r.json() if r.status_code == 200 else None
        except:
            return None

    data = {
        "cnpj": cnpj, "nome": "", "cidade": "", "uf": "", "cep": "",
        "situacao": "", "simples": "NÃO", "endereco": "", "ie": "",
        "email": "", "telefone": "", "cnae_descricao": "", "data_situacao": ""
    }

    # 1) BrasilAPI
    d = get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}")
    if d:
        data["nome"] = d.get("razao_social", "")
        data["cidade"] = d.get("municipio", "")
        data["uf"] = d.get("uf", "")
        data["cep"] = d.get("cep", "")
        data["situacao"] = d.get("descricao_situacao_cadastral", "")
        data["simples"] = "SIM" if d.get("opcao_pelo_simples") else "NÃO"
        logr = d.get("logradouro", "") or ""
        num = d.get("numero", "") or ""
        comp = d.get("complemento", "") or ""
        data["endereco"] = f"{logr}, {num} {comp}".strip().rstrip(",")
        data["email"] = d.get("email", "") or ""
        ddd = d.get("ddd_telefone_1", "") or ""
        tel = d.get("telefone_1", "") or ""
        data["telefone"] = f"({ddd}) {tel}" if ddd and tel else ""
        data["cnae_descricao"] = d.get("cnae_fiscal_descricao", "") or ""
        data["data_situacao"] = d.get("data_situacao_cadastral", "") or ""
        # IE
        data["ie"] = extrair_ie(cnpj, d)
        return data

    # 2) ReceitaWS (fallback)
    d = get(f"https://www.receitaws.com.br/v1/cnpj/{cnpj}")
    if d and d.get("status") != "ERROR":
        data["nome"] = d.get("nome", "")
        data["cidade"] = d.get("municipio", "") or d.get("cidade", "")
        data["uf"] = d.get("uf", "")
        data["cep"] = d.get("cep", "")
        data["situacao"] = d.get("situacao", "")
        data["simples"] = "SIM" if d.get("simples", {}).get("optante") else "NÃO"
        logr = d.get("logradouro", "") or ""
        num = d.get("numero", "") or ""
        comp = d.get("complemento", "") or ""
        data["endereco"] = f"{logr}, {num} {comp}".strip().rstrip(",")
        data["email"] = d.get("email", "") or ""
        data["telefone"] = d.get("telefone", "") or ""
        atv = d.get("atividade_principal", [])
        if atv and isinstance(atv, list) and len(atv) > 0:
            data["cnae_descricao"] = atv[0].get("text", "")
        data["data_situacao"] = d.get("data_situacao", "") or ""
        data["ie"] = d.get("inscricao_estadual", "") or extrair_ie(cnpj)
        return data

    # 3) CNPJ.ws (último fallback)
    d = get(f"https://publica.cnpj.ws/cnpj/{cnpj}")
    if d:
        data["nome"] = d.get("razao_social", "")
        estab = d.get("estabelecimento", {})
        data["cidade"] = estab.get("cidade", {}).get("nome", "")
        data["uf"] = estab.get("estado", {}).get("sigla", "")
        data["cep"] = estab.get("cep", "")
        data["situacao"] = estab.get("situacao_cadastral", "")
        data["simples"] = "SIM" if estab.get("simples", {}).get("optante") else "NÃO"
        logr = estab.get("logradouro", "") or ""
        num = estab.get("numero", "") or ""
        comp = estab.get("complemento", "") or ""
        data["endereco"] = f"{logr}, {num} {comp}".strip().rstrip(",")
        data["email"] = estab.get("email", "") or ""
        ddd = estab.get("ddd1", "") or ""
        tel = estab.get("telefone1", "") or ""
        data["telefone"] = f"({ddd}) {tel}" if ddd and tel else ""
        cnae = estab.get("atividade_principal", {})
        data["cnae_descricao"] = cnae.get("descricao", "") if cnae else ""
        data["data_situacao"] = estab.get("data_situacao_cadastral", "") or ""
        data["ie"] = extrair_ie(cnpj, d)
        return data

    # Se todas falharem
    data["nome"] = "ERRO NA CONSULTA"
    return data

def consultar_cep(cep):
    """Consulta um CEP na ViaCEP e retorna os dados."""
    cep = re.sub(r'\D', '', cep)
    if not cep:
        return {}
    try:
        r = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10)
        return r.json() if r.status_code == 200 else {}
    except:
        return {}

# =========================================================
# INTERFACE STREAMLIT
# =========================================================
st.sidebar.markdown("# 🏢 ConsultaPro")
st.sidebar.markdown("---")
pagina = st.sidebar.radio("Navegação", ["📋 CNPJ", "📍 CEP", "📊 Dashboard"])

# ------------------- CNPJ -------------------
if pagina == "📋 CNPJ":
    st.header("Consulta de CNPJ")
    st.markdown("Insira os CNPJs (um por linha)")

    cnpjs_input = st.text_area(
        "CNPJs",
        placeholder="00.000.000/0001-91\n12.345.678/0001-95",
        height=80,
        label_visibility="collapsed"
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        consultar_btn = st.button("🔍 Consultar", type="primary", use_container_width=True)
    with col2:
        limpar_btn = st.button("🧹 Limpar", use_container_width=True)

    if limpar_btn:
        st.session_state.pop("resultados", None)
        st.session_state.pop("df", None)
        st.rerun()

    if consultar_btn and cnpjs_input.strip():
        lista = [c.strip() for c in cnpjs_input.splitlines() if c.strip()]
        if lista:
            with st.spinner(f"Consultando {len(lista)} CNPJ(s)..."):
                resultados = []
                progress_bar = st.progress(0)
                for i, cnpj in enumerate(lista):
                    dados = consultar_cnpj(cnpj)
                    resultados.append(dados)
                    # Salvar no banco
                    conn = sqlite3.connect(DB)
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT OR REPLACE INTO cnpj
                        (cnpj, nome, cidade, uf, cep, situacao, simples, endereco, ie,
                         email, telefone, cnae_descricao, data_situacao)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        dados["cnpj"], dados["nome"], dados["cidade"], dados["uf"], dados["cep"],
                        dados["situacao"], dados["simples"], dados["endereco"], dados["ie"],
                        dados["email"], dados["telefone"], dados["cnae_descricao"], dados["data_situacao"]
                    ))
                    conn.commit()
                    conn.close()
                    progress_bar.progress((i+1)/len(lista))
                    time.sleep(0.3)
                st.success("Consulta concluída!")
                st.session_state["resultados"] = resultados
                st.session_state["df"] = pd.DataFrame(resultados)

    if "df" in st.session_state and not st.session_state["df"].empty:
        df = st.session_state["df"]
        df_resumo = df[["cnpj", "nome", "cidade", "uf", "situacao"]].copy()
        df_resumo["cidade/uf"] = df["cidade"] + "/" + df["uf"]
        df_resumo = df_resumo[["cnpj", "nome", "cidade/uf", "situacao"]]
        df_resumo.columns = ["CNPJ", "Razão Social", "Cidade/UF", "Situação"]

        st.subheader("Resultados")
        evento = st.dataframe(
            df_resumo,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        )

        if evento.selection.rows:
            idx = evento.selection.rows[0]
            selecionado = df.iloc[idx]
            st.subheader("🔎 Detalhes do CNPJ selecionado")
            with st.container(border=True):
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown(f"**CNPJ:** {selecionado['cnpj']}")
                    st.markdown(f"**Razão Social:** {selecionado['nome']}")
                    st.markdown(f"**Endereço:** {selecionado['endereco']} - CEP {selecionado['cep']}")
                    st.markdown(f"**Cidade/UF:** {selecionado['cidade']}/{selecionado['uf']}")
                    st.markdown(f"**E-mail:** {selecionado['email'] or 'Não informado'}")
                with col_b:
                    st.markdown(f"**Situação:** {selecionado['situacao']} (desde {selecionado['data_situacao']})")
                    st.markdown(f"**Simples:** {selecionado['simples']}")
                    st.markdown(f"**Inscrição Estadual:** {selecionado['ie'] or 'Não informada'}")
                    st.markdown(f"**Telefone:** {selecionado['telefone'] or 'Não informado'}")
                    st.markdown(f"**CNAE Principal:** {selecionado['cnae_descricao'] or 'Não informado'}")

# ------------------- CEP -------------------
elif pagina == "📍 CEP":
    st.header("Consulta de CEP")
    st.markdown("Insira os CEPs (um por linha)")

    ceps_input = st.text_area(
        "CEPs",
        placeholder="01001-000\n20040-020",
        height=80,
        label_visibility="collapsed"
    )

    if st.button("🔍 Consultar CEPs", type="primary"):
        lista = [c.strip() for c in ceps_input.splitlines() if c.strip()]
        if lista:
            with st.spinner(f"Consultando {len(lista)} CEP(s)..."):
                resultados_cep = []
                progress_bar = st.progress(0)
                for i, cep in enumerate(lista):
                    dados = consultar_cep(cep)
                    resultados_cep.append(dados)
                    if "erro" not in dados:
                        conn = sqlite3.connect(DB)
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT OR REPLACE INTO cep (cep, logradouro, bairro, cidade, uf)
                            VALUES (?,?,?,?,?)
                        """, (
                            dados.get("cep", ""), dados.get("logradouro", ""),
                            dados.get("bairro", ""), dados.get("localidade", ""),
                            dados.get("uf", "")
                        ))
                        conn.commit()
                        conn.close()
                    progress_bar.progress((i+1)/len(lista))
                    time.sleep(0.2)
                st.success("Consulta concluída!")
                df_cep = pd.DataFrame(resultados_cep)
                df_cep.columns = ["CEP", "Logradouro", "Complemento", "Bairro", "Cidade", "UF", "IBGE", "GIA", "DDD", "SIAFI"]
                st.dataframe(df_cep[["CEP", "Logradouro", "Bairro", "Cidade", "UF"]], use_container_width=True, hide_index=True)

# ------------------- DASHBOARD -------------------
elif pagina == "📊 Dashboard":
    st.header("Dashboard")
    conn = sqlite3.connect(DB)
    df_cnpj = pd.read_sql("SELECT * FROM cnpj", conn)
    conn.close()

    if not df_cnpj.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total de CNPJs", len(df_cnpj))
            fig1 = px.pie(df_cnpj, names="uf", title="Distribuição por UF")
            st.plotly_chart(fig1, use_container_width=True)
        with col2:
            st.metric("Total de CEPs", pd.read_sql("SELECT COUNT(*) FROM cep", sqlite3.connect(DB)).iloc[0,0])
            fig2 = px.bar(df_cnpj["situacao"].value_counts().reset_index(), x="situacao", y="count", title="Situação Cadastral")
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Últimas consultas")
        st.dataframe(df_cnpj.tail(10), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado histórico ainda. Faça uma consulta primeiro!")