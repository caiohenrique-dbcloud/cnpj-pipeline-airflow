"""
Testes unitários. Focados em lógica pura (sem rede, sem download), pra
poderem rodar em qualquer máquina/CI em segundos.

Rodar com: pytest -v
"""
import pandas as pd

from src import config


def test_situacao_cadastral_ativa_e_codigo_correto():
    """Garante que o código usado no filtro é o documentado no layout oficial da RFB."""
    assert config.SITUACAO_CADASTRAL_ATIVA == "02"


def test_matriz_filial_mapping():
    assert config.MATRIZ_FILIAL_MAP["1"] == "Matriz"
    assert config.MATRIZ_FILIAL_MAP["2"] == "Filial"


def test_colunas_estabelecimentos_tem_30_campos():
    # O layout oficial da RFB define 30 colunas para o arquivo de Estabelecimentos.
    assert len(config.COLUNAS_ESTABELECIMENTOS) == 30


def test_agregacao_matriz_filial_com_dataframe_fake():
    """
    Testa a lógica de agregação isoladamente, sem depender do DuckDB/arquivos
    reais — simula o resultado que viria da camada silver.
    """
    df = pd.DataFrame(
        {
            "identificador_matriz_filial": ["1", "2", "2", "1", "2"],
        }
    )
    df["tipo"] = df["identificador_matriz_filial"].map(config.MATRIZ_FILIAL_MAP)
    counts = df["tipo"].value_counts().to_dict()

    assert counts["Matriz"] == 2
    assert counts["Filial"] == 3
