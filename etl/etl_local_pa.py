import os
import re
import sys
import shutil
import logging
import datetime

from contextlib import closing
from ftplib import FTP, error_perm  # noqa: B402  # nosec: B402
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator, cast, Final
from urllib.request import urlopen

import pandas as pd
import numpy as np
import janitor
from dbfread import DBF, FieldParser
from more_itertools import ichunked
from pysus.utilities.readdbc import dbc2dbf
from ftplib import FTP
from typing import Generator
from frozendict import frozendict
from uuid6 import uuid7


# configura logging
def logger_config():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        handlers=[logging.StreamHandler(sys.stdout)])  
logger_config()


class LeitorCamposDBF(FieldParser):
    def parseD(self, field, data):
        # lê datas como strings
        # VER: https://dbfread.readthedocs.io/en/latest
        # /introduction.html#custom-field-types
        return self.parseC(field, data)


def _checar_arquivo_corrompido(
    tamanho_arquivo_ftp: int,
    tamanho_arquivo_local: int,
) -> bool:
    """Informa se um arquivo baixado do FTP está corrompido."""

    logging.info("Checando integridade do arquivo baixado...")
    logging.debug(
        f"Tamanho declarado do arquivo no FTP: {tamanho_arquivo_ftp} bytes",
    )
    logging.debug(
        f"Tamanho do arquivo baixado: {tamanho_arquivo_local} bytes"
    )
    if tamanho_arquivo_ftp > tamanho_arquivo_local:
        logging.error(
            "Tamanho no servidor é maior do que o do arquivo baixado.",
        )
        return True
    elif tamanho_arquivo_ftp < tamanho_arquivo_local:
        logging.error(
            "Tamanho no servidor é menor do que o do arquivo baixado.",
        )
        return True
    else:
        logging.info("OK!")
        return False


def _listar_arquivos(
    cliente_ftp: FTP,
    arquivo_nome_ou_padrao: str | re.Pattern,
) -> list[str]:
    """Busca em um diretório FTP um ou mais arquivos pelo nome ou padrão.

    Argumentos:
        cliente_ftp: Instância de conexão com o servidor FTP, já no diretório
            onde se deseja buscar os arquivos.
        arquivo_nome_ou_padrao: Nome do arquivo desejado, incluindo a
            extensão; ou expressão regular a ser comparada com os nomes de
            arquivos disponíveis no servidor FTP.

    Retorna:
        Uma lista de nomes de arquivos compatíveis com o nome ou padrão
        informados no diretório FTP.

    Exceções:
        Levanta um erro [`ftplib.error_perm`][] se nenhum arquivo
        correspondente for encontrado.

    [`ftplib.error_perm`]: https://docs.python.org/3/library/ftplib.html#ftplib.error_perm
    """

    logging.info("Listando arquivos compatíveis...")
    arquivos_todos = cliente_ftp.nlst()

    if isinstance(arquivo_nome_ou_padrao, re.Pattern):
        arquivos_compativeis = [
            arquivo
            for arquivo in arquivos_todos
            if arquivo_nome_ou_padrao.match(arquivo)
        ]
    else:
        arquivos_compativeis = [
            arquivo
            for arquivo in arquivos_todos
            if arquivo == arquivo_nome_ou_padrao
        ]

    arquivos_compativeis_num = len(arquivos_compativeis)
    if arquivos_compativeis_num > 0:
        logging.info(
            f"Encontrados {arquivos_compativeis_num} arquivos."
        )
        return arquivos_compativeis
    else:
        logging.error(
            "Nenhum arquivo compatível com o padrão fornecido foi "
            + "encontrado no diretório do servidor FTP."
        )
        raise error_perm


def extrair_dbc_lotes(
    ftp: str,
    caminho_diretorio: str,
    arquivo_nome: str | re.Pattern,
    passo: int = 10000,
    **kwargs,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai dados de um arquivo .dbc do FTP do DataSUS e retorna DataFrames.

    Dados o endereço de um FTP público do DataSUS e o caminho de um diretório
    e de um arquivo localizados nesse repositório, faz download do arquivo para
    o disco, descompacta-o e itera sobre seus registros, gerando objetos
    [`pandas.DataFrames`][] com lotes de linhas lidas.

    Argumentos:
        ftp: Endereço do repositório FTP público do DataSUS.
        caminho_diretorio: Caminho do diretório onde se encontra o arquivo
            desejado no repositório.
        arquivo_nome: Nome do arquivo no formato `.dbc` desejado, incluindo a
            extensão; ou expressão regular a ser comparada com os nomes de
            arquivos disponíveis no servidor FTP.
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.
        \*\*kwargs: Argumentos adicionais a serem passados para o construtor
            da classe
            [`dbfread.DBF`](https://dbfread.readthedocs.io/en/latest/dbf_objects.html#dbf-objects)
            ao instanciar a representação do arquivo DBF lido.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo `.dbc` desejado lido e convertido. Quando o fim do
        arquivo é atingido, os registros restantes são convertidos para
        DataFrame e a conexão com o servidor FTP é encerrada.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    logging.info(f"Conectando-se ao servidor FTP `{ftp}`...")
    cliente_ftp = FTP(ftp)
    cliente_ftp.login()
    logging.info("Conexão estabelecida com sucesso!")

    if not caminho_diretorio.startswith("/"):
        caminho_diretorio = "/" + caminho_diretorio
    logging.info(f"Buscando diretório `{caminho_diretorio}`...")
    cliente_ftp.cwd(caminho_diretorio)
    logging.info("OK!")

    arquivos_compativeis = _listar_arquivos(
        cliente_ftp=cliente_ftp,
        arquivo_nome_ou_padrao=arquivo_nome,
    )

    logging.info("Preparando ambiente para o download...")

    with TemporaryDirectory() as diretorio_temporario:
        for arquivo_compativel_nome in arquivos_compativeis:
            arquivo_dbf_nome = arquivo_compativel_nome.replace(".dbc", ".dbf")
            arquivo_dbc = Path(diretorio_temporario, arquivo_compativel_nome)
            logging.info("Tudo pronto para o download.")

            # baixar do FTP usando urllib / ver https://stackoverflow.com/a/11768443/7733563
            url = "ftp://{}{}/{}".format(
                ftp,
                caminho_diretorio,
                arquivo_compativel_nome,
            )

            logging.info(
                f"Iniciando download do arquivo `{arquivo_compativel_nome}`..."
            )
            with closing(urlopen(url)) as resposta:  # nosec: B310
                with open(arquivo_dbc, "wb") as arquivo:
                    shutil.copyfileobj(resposta, arquivo)
            logging.info("Download concluído.")

            if _checar_arquivo_corrompido(
                tamanho_arquivo_ftp=cast(
                    int,
                    cliente_ftp.size(arquivo_compativel_nome),
                ),
                tamanho_arquivo_local=arquivo_dbc.stat().st_size,
            ):
                raise RuntimeError(
                    "A extração da fonte `{}{}` ".format(
                        ftp,
                        caminho_diretorio,
                    )
                    + "falhou porque o arquivo baixado está corrompido."
                )        

            logging.info("Descompactando arquivo DBC...")
            arquivo_dbf_caminho = Path(diretorio_temporario, arquivo_dbf_nome)
            dbc2dbf(str(arquivo_dbc), str(arquivo_dbf_caminho))
            logging.info("Lendo arquivo DBF...")
            arquivo_dbf = DBF(
                arquivo_dbf_caminho,
                encoding="iso-8859-1",
                load=False,
                parserclass=LeitorCamposDBF,
                **kwargs,
            )
            arquivo_dbf_fatias = ichunked(arquivo_dbf, passo)

            contador = 0
            for fatia in arquivo_dbf_fatias:
                logging.info(
                    "Lendo trecho do arquivo DBF disponibilizado pelo DataSUS "
                    + f"e convertendo em DataFrame (linhas {contador} a {contador + passo})...",
                )
                yield arquivo_compativel_nome, contador, pd.DataFrame(fatia)
                contador += passo

    logging.debug(f"Encerrando a conexão com o servidor FTP `{ftp}`...")
    cliente_ftp.close()


def extrair_pa(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de procedimentos ambulatoriais do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos procedimentos se pretende
            obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo de procedimentos ambulatoriais lido e convertido.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    """

    arquivo_padrao = "PA{uf_sigla}{periodo_data_inicio:%y%m}[a-z]?.dbc".format(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    return extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/SIASUS/200801_/Dados",
        arquivo_nome=re.compile(arquivo_padrao, re.IGNORECASE),
        passo=passo,
    )


DE_PARA_PA: Final[frozendict] = frozendict(
    {
        "PA_CODUNI": "estabelecimento_id_scnes",
        "PA_GESTAO": "gestao_unidade_geografica_id_sus",
        "PA_CONDIC": "gestao_condicao_id_siasus",
        "PA_UFMUN": "unidade_geografica_id_sus",
        "PA_REGCT": "regra_contratual_id_scnes",
        "PA_INCOUT": "incremento_outros_id_sigtap",
        "PA_INCURG": "incremento_urgencia_id_sigtap",
        "PA_TPUPS": "estabelecimento_tipo_id_sigtap",
        "PA_TIPPRE": "prestador_tipo_id_sigtap",
        "PA_MN_IND": "estabelecimento_mantido",
        "PA_CNPJCPF": "estabelecimento_id_cnpj",
        "PA_CNPJMNT": "mantenedora_id_cnpj",
        "PA_CNPJ_CC": "receptor_credito_id_cnpj",
        "PA_MVM": "processamento_periodo_data_inicio",
        "PA_CMP": "realizacao_periodo_data_inicio",
        "PA_PROC_ID": "procedimento_id_sigtap",
        "PA_TPFIN": "financiamento_tipo_id_sigtap",
        "PA_SUBFIN": "financiamento_subtipo_id_sigtap",
        "PA_NIVCPL": "complexidade_id_siasus",
        "PA_DOCORIG": "instrumento_registro_id_siasus",
        "PA_AUTORIZ": "autorizacao_id_siasus",
        "PA_CNSMED": "profissional_id_cns",
        "PA_CBOCOD": "profissional_vinculo_ocupacao_id_cbo2002",
        "PA_MOTSAI": "desfecho_motivo_id_siasus",
        "PA_OBITO": "obito",
        "PA_ENCERR": "encerramento",
        "PA_PERMAN": "permanencia",
        "PA_ALTA": "alta",
        "PA_TRANSF": "transferencia",
        "PA_CIDPRI": "condicao_principal_id_cid10",
        "PA_CIDSEC": "condicao_secundaria_id_cid10",
        "PA_CIDCAS": "condicao_associada_id_cid10",
        "PA_CATEND": "carater_atendimento_id_siasus",
        "PA_IDADE": "usuario_idade",
        "IDADEMIN": "procedimento_idade_minima",
        "IDADEMAX": "procedimento_idade_maxima",
        "PA_FLIDADE": "compatibilidade_idade_id_siasus",
        "PA_SEXO": "usuario_sexo_id_sigtap",
        "PA_RACACOR": "usuario_raca_cor_id_siasus",
        "PA_MUNPCN": "usuario_residencia_municipio_id_sus",
        "PA_QTDPRO": "quantidade_apresentada",
        "PA_QTDAPR": "quantidade_aprovada",
        "PA_VALPRO": "valor_apresentado",
        "PA_VALAPR": "valor_aprovado",
        "PA_UFDIF": "atendimento_residencia_ufs_distintas",
        "PA_MNDIF": "atendimento_residencia_municipios_distintos",
        "PA_DIF_VAL": "procedimento_valor_diferenca_sigtap",
        "NU_VPA_TOT": "procedimento_valor_vpa",
        "NU_PA_TOT": "procedimento_valor_sigtap",
        "PA_INDICA": "aprovacao_status_id_siasus",
        "PA_CODOCO": "ocorrencia_id_siasus",
        "PA_FLQT": "erro_quantidade_apresentada_id_siasus",
        "PA_FLER": "erro_apac",
        "PA_ETNIA": "usuario_etnia_id_sus",
        "PA_VL_CF": "complemento_valor_federal",
        "PA_VL_CL": "complemento_valor_local",
        "PA_VL_INC": "incremento_valor",
        "PA_SRV_C": "servico_especializado_id_scnes",
        "PA_INE": "equipe_id_ine",
        "PA_NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

TIPOS_PA: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_scnes": "object",
        "gestao_unidade_geografica_id_sus": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "regra_contratual_id_scnes": "object",
        "incremento_outros_id_sigtap": "object",
        "incremento_urgencia_id_sigtap": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_id_cnpj": "object",
        "mantenedora_id_cnpj": "object",
        "receptor_credito_id_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "procedimento_id_sigtap": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "complexidade_id_siasus": "object",
        "instrumento_registro_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_id_cns": "object",
        "profissional_vinculo_ocupacao_id_cbo2002": "object",
        "desfecho_motivo_id_siasus": "object",
        "obito": "bool",
        "encerramento": "bool",
        "permanencia": "bool",
        "alta": "bool",
        "transferencia": "bool",
        "condicao_principal_id_cid10": "object",
        "condicao_secundaria_id_cid10": "object",
        "condicao_associada_id_cid10": "object",
        "carater_atendimento_id_siasus": "object",
        "usuario_idade": "Int64",
        "procedimento_idade_minima": "Int64",
        "procedimento_idade_maxima": "Int64",
        "compatibilidade_idade_id_siasus": "object",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "valor_apresentado": "Float64",
        "valor_aprovado": "Float64",
        "atendimento_residencia_ufs_distintas": "bool",
        "atendimento_residencia_municipios_distintos": "bool",
        "procedimento_valor_diferenca_sigtap": "Float64",
        "procedimento_valor_vpa": "Float64",
        "procedimento_valor_sigtap": "Float64",
        "aprovacao_status_id_siasus": "object",
        "ocorrencia_id_siasus": "object",
        "erro_quantidade_apresentada_id_siasus": "object",
        "erro_apac": "object",
        "usuario_etnia_id_sus": "object",
        "complemento_valor_federal": "Float64",
        "complemento_valor_local": "Float64",
        "incremento_valor": "Float64",
        "servico_id_sigtap": "object",
        "servico_classificacao_id_sigtap": "object",
        "equipe_id_ine": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": "object",
    },
)

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_PA.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "0":
        return False
    elif valor == "1":
        return True
    else:
        return np.nan


def transformar_pa(
    pa: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de procedimentos ambulatoriais do SIASUS.
    
    Argumentos:
        pa: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo de
            disseminação de procedimentos ambulatoriais do SIASUS, conforme
            extraídos para uma unidade federativa e competência (mês) pela
            função [`extrair_pa()`][].

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.
    
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """
    logging.info(
        f"Transformando DataFrame com {len(pa)} procedimentos "
        + "ambulatoriais."
    )
    memoria_usada=pa.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )

    # aplica condições de filtragem dos registros
    condicoes = "(PA_TPUPS == '70') or PA_PROC_ID.str.startswith('030106') or PA_PROC_ID.str.startswith('030107') or PA_PROC_ID.str.startswith('030108') or PA_CIDPRI.str.startswith('F') or PA_CIDPRI.str.startswith('F') or PA_CIDPRI.str.startswith('X6') or PA_CIDPRI.str.startswith('X7') or PA_CIDPRI.str.contains('^X8[0-4][0-9]*') or PA_CIDPRI.str.startswith('R78') or PA_CIDPRI.str.startswith('T40') or (PA_CIDPRI == 'Y870') or PA_CIDPRI.str.startswith('Y90') or PA_CIDPRI.str.startswith('Y91') or (PA_CBOCOD in ['223905', '223915', '225133', '223550', '239440', '239445', '322220']) or PA_CBOCOD.str.startswith('2515') or (PA_CATEND == '02')"
    logging.info(
        f"Filtrando DataFrame com {len(pa)} procedimentos ambulatoriais.",
    )
    pa = pa.query(condicoes, engine="python")
    logging.info(
        f"Registros após aplicar filtro: {len(pa)}."
    )

    pa_transformada = (
        pa  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_PA)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "regra_contratual_id_scnes",
                "incremento_outros_id_sigtap",
                "incremento_urgencia_id_sigtap",
                "mantenedora_id_cnpj",
                "receptor_credito_id_cnpj",
                "financiamento_subtipo_id_sigtap",
                "condicao_principal_id_cid10",
                "autorizacao_id_siasus",
                "profissional_id_cns",
                "condicao_principal_id_cid10",
                "condicao_secundaria_id_cid10",
                "condicao_associada_id_cid10",
                "desfecho_motivo_id_siasus",
                "usuario_sexo_id_sigtap",
                "usuario_raca_cor_id_siasus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        .transform_columns(
            [
                "carater_atendimento_id_siasus",
                "usuario_residencia_municipio_id_sus",
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=lambda elemento: (
                np.nan
                if pd.isna(elemento)
                or all(digito == "9" for digito in elemento)
                else elemento
            ),
        )
        .update_where(
            "usuario_idade == '999'",
            target_column_name="usuario_idade",
            target_val=np.nan,
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "M" else False,
        )
        .transform_columns(
            [
                "obito",
                "encerramento",
                "permanencia",
                "alta",
                "transferencia",
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=_para_booleano,
        )
        .update_where(
            "@pd.isna(desfecho_motivo_id_siasus)",
            target_column_name=[
                "obito",
                "encerramento",
                "permanencia",
                "alta",
                "transferencia",
            ],
            target_val=np.nan,
        )
        # separar código do serviço e código da classificação do serviço
        .transform_column(
            "servico_especializado_id_scnes",
            function=lambda cod: cod[:3] if pd.notna(cod) else np.nan,
            dest_column_name="servico_id_sigtap",
        )
        .transform_column(
            "servico_especializado_id_scnes",
            function=lambda cod: cod[3:] if pd.notna(cod) else np.nan,
            dest_column_name="servico_classificacao_id_sigtap",
        )
        .remove_columns("servico_especializado_id_scnes")
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)

        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_PA)
    )
    memoria_usada = pa_transformada.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(        
        f"Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."
    )
    return pa_transformada



def baixar_e_processar_pa(
        uf_sigla: str, 
        periodo_data_inicio: datetime.date, 
        diretorio: str,
        ):
    """Baixa e processa dados de procedimentos ambulatoriais do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos procedimentos se pretende obter.

        periodo_data_inicio (datetime.date): A data de início do período de tempo para o qual os dados serão obtidos. 
        Deve ser fornecida como um objeto `datetime.date`.

        diretorio: Caminho completo para a pasta onde se pretende salvar o arquivo final após processamento.

    Retorna:
        Um objeto [`pandas.DataFrame`][] contendo os dados de procedimentos ambulatoriais para a Unidade Federativa
        e competência (mês) informados, após aplicação de transformações e filtros específicos.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects

    Exceções:
        Levanta um erro [`RuntimeError`][] se o arquivo baixado estiver corrompido ou não puder ser processado.

    [`RuntimeError`]: https://docs.python.org/3/library/exceptions.html#RuntimeError
    """

    # Extrair dados
    df_dados_todos = []
    arquivos_capturados = []
    for arquivo_dbc, _, df_dados in extrair_pa(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    ):
        df_dados = transformar_pa(df_dados)
        df_dados_todos.append(df_dados)
        arquivos_capturados.append(arquivo_dbc)

    # Concatenar DataFrames
    logging.info("Concatenando resultados em arquivo único.")
    df_dados_final = pd.concat(df_dados_todos)

    # Salvar localmente
    logging.info("Salvando localmente...")
    nome_arquivo_csv = f"siasus_procedimentos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    caminho_completo = os.path.join(diretorio, nome_arquivo_csv)
    df_dados_final.to_csv(caminho_completo, index=False)


    response = {
        "status": "OK",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "arquivos_origem_dbc": list(set(arquivos_capturados)),
        "local": caminho_completo,
    }

    logging.info(
        f"Processamento de PA finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Salvo em {response['local']}"
    )

    return response


