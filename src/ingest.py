"""
Camada BRONZE (Landing): ingestão automatizada dos dados abertos do CNPJ.

Responsabilidades:
  1. Dado um mês/ano, localizar a pasta correta no site da Receita Federal.
  2. Fazer fallback para o mês anterior se o mês pedido ainda não foi publicado
     (isso resolve o requisito "Dezembro/2025 ou o mês mais recente disponível").
  3. Baixar apenas os arquivos necessários: Estabelecimentos*.zip e Municipios.zip
     (não baixamos Empresas/Socios/etc. porque a pergunta de negócio não precisa
     deles — isso já economiza dezenas de GB de download desnecessário).
  4. Gravar os .zip brutos, sem qualquer transformação, na camada bronze.

Idempotência: antes de baixar, verificamos se o arquivo já existe no bronze
para aquele ano/mês. Se existir, o download é pulado (a não ser que --force
seja usado no pipeline).
"""
import logging
import random
import re
import time
import zipfile
from datetime import date
from pathlib import Path

import requests

from . import config
from .storage import Storage

logger = logging.getLogger(__name__)

TMP_DIR = config.ROOT_DIR / ".tmp_downloads"


class ReferenceNotAvailable(Exception):
    """Levantada quando nenhum mês candidato tem dados publicados."""


def _list_remote_files(base_url: str, year: int, month: int) -> list[str] | None:
    """Retorna a lista de arquivos disponíveis na pasta YYYY-MM/, ou None se a
    pasta não existir (404) ou o domínio estiver indisponível."""
    folder_url = f"{base_url}/{year:04d}-{month:02d}/"
    try:
        resp = requests.get(folder_url, timeout=15)
    except requests.exceptions.RequestException as exc:
        logger.warning("Domínio indisponível (%s): %s", base_url, exc)
        return None
    if resp.status_code != 200:
        return None

    # O índice da RFB é uma página HTML simples com <a href="Arquivo.zip">.
    files = sorted(set(re.findall(r'href="([\w\-.]+\.zip)"', resp.text)))
    return files or None


def _list_mirror_folder_and_files(year: int, month: int) -> tuple[str, list[str]] | None:
    """
    O mirror (Casa dos Dados) nomeia as pastas por DATA DE PUBLICAÇÃO
    ("YYYY-MM-DD/"), não por "YYYY-MM/" como os domínios oficiais. Por isso
    primeiro descobrimos qual subpasta corresponde ao ano/mês pedido, e só
    depois listamos os arquivos dentro dela.

    Retorna (folder_url, arquivos) ou None se não achar nada para o período.
    """
    try:
        resp = requests.get(f"{config.MIRROR_INDEX_URL}/", timeout=15)
    except requests.exceptions.RequestException as exc:
        logger.warning("Mirror indisponível: %s", exc)
        return None
    if resp.status_code != 200:
        return None

    prefix = f"{year:04d}-{month:02d}-"
    candidates = sorted(set(re.findall(rf'href="({re.escape(prefix)}\d{{2}}/)"', resp.text)))
    if not candidates:
        return None

    folder_url = f"{config.MIRROR_INDEX_URL}/{candidates[-1]}"
    try:
        resp = requests.get(folder_url, timeout=15)
    except requests.exceptions.RequestException:
        return None
    if resp.status_code != 200:
        return None

    files = sorted(set(re.findall(r'href="([\w\-.]+\.zip)"', resp.text)))
    return (folder_url, files) if files else None


def resolve_reference_period(year: int, month: int, max_lookback: int = 3) -> tuple[int, int, str, list[str]]:
    """
    Encontra o primeiro período (year, month) com dados publicados, começando
    no período pedido e voltando no tempo se necessário.

    Tenta, nessa ordem: (1) domínios oficiais da RFB, (2) mirror da Casa dos
    Dados — usado quando os oficiais estão fora do ar/instáveis, o que é
    comum nesse portal.

    Retorna (year, month, base_url, arquivos_disponiveis). O base_url pode
    ser um domínio oficial (pasta "YYYY-MM/") ou a pasta já resolvida do
    mirror (pasta "YYYY-MM-DD/") — o restante do código não precisa saber
    a diferença, pois ambos retornam uma URL "pronta" para prefixar o nome
    do arquivo.
    """
    ref = date(year, month, 1)
    for _ in range(max_lookback + 1):
        for base_url in config.BASE_URL_CANDIDATES:
            files = _list_remote_files(base_url, ref.year, ref.month)
            if files:
                resolved_base = f"{base_url}/{ref.year:04d}-{ref.month:02d}"
                logger.info("Período disponível encontrado: %04d-%02d (%s)", ref.year, ref.month, base_url)
                return ref.year, ref.month, resolved_base, files

        mirror_result = _list_mirror_folder_and_files(ref.year, ref.month)
        if mirror_result:
            folder_url, files = mirror_result
            logger.info("Período disponível encontrado: %04d-%02d (mirror: %s)", ref.year, ref.month, folder_url)
            return ref.year, ref.month, folder_url.rstrip("/"), files

        # volta um mês
        prev_month = ref.month - 1 or 12
        prev_year = ref.year - 1 if ref.month == 1 else ref.year
        ref = date(prev_year, prev_month, 1)

    raise ReferenceNotAvailable(
        f"Nenhum período publicado encontrado (domínios oficiais e mirror) entre "
        f"{year:04d}-{month:02d} e {max_lookback} meses anteriores."
    )


def _download_file(url: str, dest: Path, max_attempts: int = 30) -> Path:
    """
    Baixa um arquivo com retry automático E retomada de download (resume).

    Por que isso existe: em conexões instáveis, um arquivo de ~2GB pode
    quebrar várias vezes seguidas, às vezes depois de já ter baixado mais
    de 70% do conteúdo. Descartar tudo e recomeçar do zero a cada queda
    desperdiça tempo e dados — especialmente crítico em ambientes com
    internet doméstica instável (o cenário real deste projeto).

    Como funciona: gravamos em um arquivo temporário (.partial). Se uma
    tentativa cair no meio, a PRÓXIMA tentativa pede ao servidor "me manda
    só a partir do byte X" (cabeçalho HTTP Range), em vez de o arquivo
    inteiro de novo. A maioria dos servidores de arquivos estáticos (como
    o mirror usado aqui, atrás de CDN) suporta isso — confirmado pelo
    cabeçalho de resposta 206 Partial Content.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial_path = dest.with_name(dest.name + ".partial")

    last_error = None
    for attempt in range(1, max_attempts + 1):
        resume_from = partial_path.stat().st_size if partial_path.exists() else 0
        headers = {"Range": f"bytes={resume_from}-"} if resume_from > 0 else {}

        try:
            if resume_from > 0:
                logger.info(
                    "Retomando download de %s a partir do byte %d (tentativa %d/%d) ...",
                    url, resume_from, attempt, max_attempts,
                )
            else:
                logger.info("Baixando %s (tentativa %d/%d) ...", url, attempt, max_attempts)

            with requests.get(url, stream=True, timeout=(10, 30), headers=headers) as resp:
                if resume_from > 0 and resp.status_code != 206:
                    # Servidor não suportou o resume (ignorou o Range) — descarta
                    # o parcial e recomeça do zero para não corromper o arquivo.
                    logger.warning("Servidor não suportou retomada; reiniciando este arquivo do zero.")
                    partial_path.unlink(missing_ok=True)
                    resume_from = 0

                resp.raise_for_status()
                mode = "ab" if resume_from > 0 else "wb"
                # Chunks pequenos (1MB, em vez de 8MB): em conexões muito
                # instáveis, isso significa perder no máximo ~1MB de progresso
                # a cada queda, em vez de até 8MB — e detecta a queda mais rápido,
                # já que cada chunk é uma oportunidade de a conexão falhar e ser
                # percebida, ao invés de ficar "pendurada" esperando um chunk grande.
                with open(partial_path, mode) as f:
                    for chunk in resp.iter_content(chunk_size=1 * 1024 * 1024):
                        f.write(chunk)

            partial_path.replace(dest)  # só renomeia para o nome final quando completo
            return dest

        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            downloaded_so_far = partial_path.stat().st_size if partial_path.exists() else 0
            logger.warning(
                "Download interrompido no meio (tentativa %d/%d, %d bytes salvos até agora): %s",
                attempt, max_attempts, downloaded_so_far, exc,
            )
            # NÃO apaga o .partial aqui — é exatamente o que permite retomar
            # na próxima tentativa, em vez de perder o progresso já feito.
            time.sleep(min(5 * attempt, 20))

    raise RuntimeError(f"Falha ao baixar {url} após {max_attempts} tentativas.") from last_error


def _generate_mock_bronze_zips(tmp_dir: Path) -> dict[str, Path]:
    """
    Gera arquivos .zip minúsculos e sintéticos, com EXATAMENTE o mesmo layout
    de colunas dos dados reais da RFB, para testar a pipeline inteira
    (Bronze -> Silver -> Gold, incluindo o Airflow orquestrando tudo) sem
    depender de internet nenhuma vez.

    Por quê isso existe: os arquivos reais passam de 1GB cada, e em conexões
    instáveis isso pode levar dezenas de minutos ou quebrar repetidamente
    (ver seção "Desafios enfrentados" do README). Esse modo isola o teste
    da ORQUESTRAÇÃO (Airflow, DAG, retries, XCom) do teste do DOWNLOAD real
    — são preocupações diferentes, e não faz sentido depender de uma para
    validar a outra.

    Os dados gerados são fictícios, mas com contagens conhecidas de
    Matriz/Filial ativas em São Paulo, então dá pra conferir se o resultado
    final bate com o esperado.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    codigo_sp = "9999"       # código fictício de município para os testes
    codigo_outra_cidade = "8888"

    def _linha_estabelecimento(matriz_filial: str, situacao: str, municipio: str, idx: int) -> str:
        campos = {
            "cnpj_basico": f"{idx:08d}",
            "cnpj_ordem": "0001",
            "cnpj_dv": "00",
            "identificador_matriz_filial": matriz_filial,
            "nome_fantasia": f"EMPRESA TESTE {idx}",
            "situacao_cadastral": situacao,
            "data_situacao_cadastral": "20200101",
            "motivo_situacao_cadastral": "00",
            "nome_cidade_exterior": "",
            "pais": "",
            "data_inicio_atividade": "20180101",
            "cnae_fiscal_principal": "6201500",
            "cnae_fiscal_secundaria": "",
            "tipo_logradouro": "RUA",
            "logradouro": "TESTE",
            "numero": "123",
            "complemento": "",
            "bairro": "CENTRO",
            "cep": "01000000",
            "uf": "SP",
            "codigo_municipio": municipio,
            "ddd1": "11",
            "telefone1": "999999999",
            "ddd2": "",
            "telefone2": "",
            "ddd_fax": "",
            "fax": "",
            "email": "",
            "situacao_especial": "",
            "data_situacao_especial": "",
        }
        return ";".join(campos[nome] for nome in config.COLUNAS_ESTABELECIMENTOS)

    linhas = []
    # 12 matrizes ativas em SP (a "resposta" esperada do teste)
    for i in range(12):
        linhas.append(_linha_estabelecimento("1", config.SITUACAO_CADASTRAL_ATIVA, codigo_sp, i))
    # 7 filiais ativas em SP
    for i in range(12, 19):
        linhas.append(_linha_estabelecimento("2", config.SITUACAO_CADASTRAL_ATIVA, codigo_sp, i))
    # ruído: registros que NÃO deveriam entrar no resultado, para validar o filtro
    linhas.append(_linha_estabelecimento("1", "08", codigo_sp, 90))              # SP, mas Baixada
    linhas.append(_linha_estabelecimento("2", config.SITUACAO_CADASTRAL_ATIVA, codigo_outra_cidade, 91))  # Ativa, mas fora de SP

    random.Random(42).shuffle(linhas)  # ordem embaralhada, como nos dados reais
    estab_csv = ("\n".join(linhas) + "\n").encode("latin-1")

    municipios_csv = f"{codigo_sp};SAO PAULO\n{codigo_outra_cidade};RIO DE JANEIRO\n".encode("latin-1")

    estab_zip = tmp_dir / "Estabelecimentos0.zip"
    municipios_zip = tmp_dir / "Municipios.zip"

    with zipfile.ZipFile(estab_zip, "w") as zf:
        zf.writestr("MOCK_ESTABELE.csv", estab_csv)
    with zipfile.ZipFile(municipios_zip, "w") as zf:
        zf.writestr("MOCK_MUNIC.csv", municipios_csv)

    logger.warning(
        "Modo MOCK ativado: gerando dados sintéticos (12 matrizes + 7 filiais "
        "ativas em SP esperadas), sem acessar a internet."
    )
    return {"Estabelecimentos0.zip": estab_zip, "Municipios.zip": municipios_zip}



def run_bronze_ingestion(
    year: int, month: int, storage: Storage, force: bool = False,
    max_estab_files: int | None = None, mock: bool = False,
) -> dict:
    """
    Executa a ingestão bronze para o período pedido (com fallback automático).

    max_estab_files: se informado, baixa apenas essa quantidade de arquivos
    "Estabelecimentos*.zip" (em vez dos ~10 disponíveis). Útil para testar o
    pipeline ponta a ponta rapidamente, sem esperar o download completo do
    mês inteiro. Os resultados finais ficam parciais (uma amostra da cidade),
    mas a lógica é exatamente a mesma — é só uma questão de volume de dados.

    mock: se True, NÃO acessa a internet — gera dados sintéticos localmente
    (ver _generate_mock_bronze_zips) para validar a orquestração (Airflow,
    DAG, retries) sem depender da estabilidade da fonte real. Use isso para
    testar rapidamente; use False para o resultado de negócio de verdade.

    Retorna um dicionário com metadados úteis para as próximas camadas, entre
    eles o ano/mês efetivamente usado (pode diferir do pedido, por causa do
    fallback) e as keys dos arquivos no bronze.
    """
    if mock:
        # IMPORTANTE: usamos um prefixo isolado (MOCK-...) em vez de
        # "{year}-{month}" comum. Sem isso, os arquivos fictícios do mock
        # ocupavam o MESMO endereço no bronze que os dados reais usariam
        # (ex: "2025-12/Estabelecimentos0.zip") — e a idempotência então
        # "enganava" uma execução real seguinte, fazendo-a pular o download
        # de verdade por pensar que já existia (quando na verdade era o
        # mock). Esse isolamento evita esse tipo de contaminação silenciosa.
        prefix = config.build_prefix(year, month, mock=True)
        mock_files = _generate_mock_bronze_zips(TMP_DIR / prefix / "mock")
        downloaded_keys = []
        for filename, local_path in mock_files.items():
            key = f"{prefix}/{filename}"
            storage.upload_file(config.BUCKET_BRONZE, key, local_path)
            downloaded_keys.append(key)
        return {
            "year": year,
            "month": month,
            "mock": True,
            "estabelecimento_keys": [k for k in downloaded_keys if "Estabelecimentos" in k],
            "municipio_keys": [k for k in downloaded_keys if "Municipios" in k],
        }

    resolved_year, resolved_month, base_url, remote_files = resolve_reference_period(year, month)

    estabelecimento_files = sorted(f for f in remote_files if f.startswith("Estabelecimentos"))
    municipio_files = [f for f in remote_files if f.startswith("Municipios")]

    if max_estab_files is not None:
        estabelecimento_files = estabelecimento_files[:max_estab_files]
        logger.warning(
            "Modo amostra ativado: baixando apenas %d de %d arquivos de Estabelecimentos.",
            len(estabelecimento_files), max_estab_files,
        )

    if not estabelecimento_files or not municipio_files:
        raise ReferenceNotAvailable(
            f"Período {resolved_year:04d}-{resolved_month:02d} está incompleto na fonte "
            "(faltam arquivos de Estabelecimentos ou Municipios)."
        )

    prefix = config.build_prefix(resolved_year, resolved_month, mock=False)
    downloaded_keys = []

    for filename in estabelecimento_files + municipio_files:
        key = f"{prefix}/{filename}"
        if not force and storage.exists(config.BUCKET_BRONZE, key):
            logger.info("Já existe no bronze, pulando: %s", key)
            downloaded_keys.append(key)
            continue

        tmp_path = TMP_DIR / prefix / filename
        _download_file(f"{base_url}/{filename}", tmp_path)
        storage.upload_file(config.BUCKET_BRONZE, key, tmp_path)
        downloaded_keys.append(key)

    return {
        "year": resolved_year,
        "month": resolved_month,
        "estabelecimento_keys": [k for k in downloaded_keys if "Estabelecimentos" in k],
        "municipio_keys": [k for k in downloaded_keys if "Municipios" in k],
    }
