import streamlit as st
import pandas as pd
import sqlite3
import requests
import time
import re
import json
import plotly.express as px
from io import BytesIO

# =========================================================
# CONFIGURAÇÃO DA PÁGINA (com logo como favicon)
# =========================================================
st.set_page_config(
    page_title="ConsultaPro",
    page_icon="logo.png",   # Substitua pelo nome do seu arquivo de logo
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
    # Tabela CNPJ
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
    # Adiciona coluna qsa_json se não existir (upgrade)
    try:
        cur.execute("ALTER TABLE cnpj ADD COLUMN qsa_json TEXT")
    except sqlite3.OperationalError:
        pass

    # Tabela CEP
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
    """Tenta extrair a Inscrição Estadual."""
    if dados_api:
        estab = dados_api.get("estabelecimento")
        if isinstance(estab, dict):
            if estab.get("inscricao_estadual"):
                return estab.get("inscricao_estadual")
            ies = estab.get("inscricoes_estaduais")
            if isinstance(ies, list):
                for item in ies:
                    ie = item.get("inscricao_estadual")
                    if ie:
                        return ie

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
    return ""

def consultar_cnpj(cnpj):
    """Consulta um CNPJ nas APIs públicas."""
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
        "email": "", "telefone": "", "cnae_descricao": "", "data_situacao": "",
        "qsa_json": "[]"
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

        qsa_list = d.get("qsa", [])
        data["qsa_json"] = json.dumps(qsa_list, ensure_ascii=False) if qsa_list else "[]"

        data["ie"] = extrair_ie(cnpj, d)
        return data

    # 2) ReceitaWS
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

        qsa_list = d.get("qsa", [])
        data["qsa_json"] = json.dumps(qsa_list, ensure_ascii=False) if qsa_list else "[]"
        return data

    # 3) CNPJ.ws
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

        socios = d.get("socios", [])
        qsa_list = []
        for s in socios:
            qsa_list.append({
                "nome_socio": s.get("nome", ""),
                "qualificacao_socio": s.get("qualificacao", ""),
                "data_entrada_sociedade": s.get("data_entrada", "")
            })
        data["qsa_json"] = json.dumps(qsa_list, ensure_ascii=False) if qsa_list else "[]"
        return data

    data["nome"] = "ERRO NA CONSULTA"
    return data

def consultar_cep(cep):
    """Consulta um CEP na ViaCEP."""
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

# --- SIDEBAR COM LOGO ---
col1, col2, col3 = st.sidebar.columns([1, 2, 1])
with col2:
    st.image("logo.png", use_column_width=True)  # Substitua pelo nome do seu arquivo
st.sidebar.markdown("<h3 style='text-align: center;'>ConsultaPro</h3>", unsafe_allow_html=True)
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
                    conn = sqlite3.connect(DB)
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT OR REPLACE INTO cnpj
                        (cnpj, nome, cidade, uf, cep, situacao, simples, endereco, ie,
                         email, telefone, cnae_descricao, data_situacao, qsa_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        dados["cnpj"], dados["nome"], dados["cidade"], dados["uf"], dados["cep"],
                        dados["situacao"], dados["simples"], dados["endereco"], dados["ie"],
                        dados["email"], dados["telefone"], dados["cnae_descricao"], dados["data_situacao"],
                        dados["qsa_json"]
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

        # Botão de exportação
        st.markdown("---")
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_export = df.drop(columns=['qsa_json'], errors='ignore')
            df_export.to_excel(writer, sheet_name='CNPJs', index=False)
        output.seek(0)
        col_exp1, _ = st.columns([1, 4])
        with col_exp1:
            st.download_button(
                label="📥 Exportar Excel",
                data=output,
                file_name="consulta_cnpj.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
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

                # QSA
                qsa_str = selecionado.get('qsa_json', '[]')
                try:
                    socios = json.loads(qsa_str)
                    if socios:
                        st.markdown("---")
                        st.subheader("👥 Quadro de Sócios e Administradores (QSA)")
                        df_socios = pd.DataFrame(socios)
                        colunas_qsa = ['nome_socio', 'qualificacao_socio', 'data_entrada_sociedade']
                        colunas_existentes = [c for c in colunas_qsa if c in df_socios.columns]
                        if colunas_existentes:
                            df_socios = df_socios[colunas_existentes]
                            df_socios.columns = ['Nome do Sócio', 'Qualificação', 'Data de Entrada']
                            st.dataframe(df_socios, use_container_width=True, hide_index=True)
                        else:
                            st.info("Dados de QSA em formato desconhecido.")
                    else:
                        st.info("Nenhum dado de QSA disponível.")
                except json.JSONDecodeError:
                    st.warning("Erro ao processar os dados do QSA.")

                # --- BOTÕES PARA CENPROT E SERASA (INTELIGENTES) ---
                st.markdown("---")
                st.subheader("🔍 Consultas de Inadimplência (Links Externos)")

                # Prepara o CNPJ limpo e identifica o estado
                cnpj_limpo = re.sub(r'\D', '', selecionado['cnpj'])
                uf_empresa = str(selecionado.get('uf', '')).upper()

                # Define a URL do CENPROT com base na UF
                if uf_empresa == 'SP':
                    url_cenprot = f"https://www.protestosp.com.br/consulta-gratuita-de-protesto?documento={cnpj_limpo}"
                else:
                    url_cenprot = f"https://site.cenprotnacional.org.br/?cpfcnpj={cnpj_limpo}"

                # URL da Serasa
                url_serasa = f"https://empresas.serasaexperian.com.br/consulta-gratis?cnpj={cnpj_limpo}"

                # Cria os botões lado a lado
                col_b1, col_b2 = st.columns(2)
                with col_b1:
                    st.link_button("📋 Consultar no CENPROT", url_cenprot, use_container_width=True)
                with col_b2:
                    st.link_button("📊 Consultar no Serasa", url_serasa, use_container_width=True)

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
                    if dados and "cep" in dados and "erro" not in dados:
                        item = {
                            "CEP": dados.get("cep", ""),
                            "Logradouro": dados.get("logradouro", ""),
                            "Bairro": dados.get("bairro", ""),
                            "Cidade": dados.get("localidade", ""),
                            "UF": dados.get("uf", "")
                        }
                        resultados_cep.append(item)
                        conn = sqlite3.connect(DB)
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT OR REPLACE INTO cep (cep, logradouro, bairro, cidade, uf)
                            VALUES (?,?,?,?,?)
                        """, (item["CEP"], item["Logradouro"], item["Bairro"], item["Cidade"], item["UF"]))
                        conn.commit()
                        conn.close()
                    else:
                        resultados_cep.append({
                            "CEP": cep,
                            "Logradouro": "CEP inválido ou não encontrado",
                            "Bairro": "-",
                            "Cidade": "-",
                            "UF": "-"
                        })
                    progress_bar.progress((i+1)/len(lista))
                    time.sleep(0.2)
                st.success("Consulta concluída!")
                if resultados_cep:
                    df_cep = pd.DataFrame(resultados_cep)
                    st.dataframe(df_cep, use_container_width=True, hide_index=True)
                else:
                    st.warning("Nenhum CEP válido foi retornado.")

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
            if "uf" in df_cnpj.columns and not df_cnpj["uf"].isnull().all():
                fig1 = px.pie(df_cnpj, names="uf", title="Distribuição por UF")
                st.plotly_chart(fig1, use_container_width=True)
        with col2:
            conn_cep = sqlite3.connect(DB)
            total_cep = pd.read_sql("SELECT COUNT(*) FROM cep", conn_cep).iloc[0,0]
            conn_cep.close()
            st.metric("Total de CEPs", total_cep)
            if "situacao" in df_cnpj.columns:
                situacao_counts = df_cnpj["situacao"].value_counts().reset_index()
                situacao_counts.columns = ["situacao", "count"]
                fig2 = px.bar(situacao_counts, x="situacao", y="count", title="Situação Cadastral")
                st.plotly_chart(fig2, use_container_width=True)

        st.subheader("📋 Últimas consultas")
        colunas_exibir = ["cnpj", "nome", "cidade", "uf", "situacao"]
        colunas_disponiveis = [c for c in colunas_exibir if c in df_cnpj.columns]
        df_hist = df_cnpj[colunas_disponiveis].tail(10).copy()
        df_hist.columns = ["CNPJ", "Razão Social", "Cidade", "UF", "Situação"]
        st.dataframe(df_hist, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado histórico ainda. Faça uma consulta primeiro!")