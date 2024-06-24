# Projeto Aplicado - Pós graduação em engenharia de dados | IGTI XPEducação

![Badge Concluído](https://img.shields.io/badge/status-concluido-green)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Flask](https://img.shields.io/badge/flask-%23000.svg?style=for-the-badge&logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)


Esse repositório tem como objetivo hospedar os códigos de extração, tratamento e carregamento da implementação de ETL simplificado de dados de Procedimentos Ambulatoriais relacionados a Saúde Mental para o projeto aplicado da pós graduação em Engenharia de dados, bem como desenhos de arquitetura e arquivo de texto completo do Projeto Aplicado.

*******
## :mag_right: Índice
1. [Contexto](#contexto)
2. [Estrutura do repositório](#estrutura)
4. [Instruções para instalação](#instalacao)
    1. [Instalação via código fonte](#local)
    2. [Obtendo e construindo a imagem Docker](#docker)
5. [Utilização](#utilizacao)
*******

<div id='contexto'/>  

## :rocket: Contexto

### Sobre a Rede de Atenção Psicossocial (RAPS)
A Rede de Atenção Psicossocial (RAPS) foi criada em 1990 no Brasil como parte do Sistema Único de Saúde (SUS) e da reforma psiquiátrica iniciada nos anos 1980. Seu objetivo é substituir a internação hospitalar de pessoas com transtornos mentais por atendimentos psicológicos e outras atividades menos invasivas, promovendo a cidadania, o fortalecimento de vínculos sociais e a autonomia dos usuários. A RAPS segue a lógica de territorialização do SUS, organizando-se para atender às necessidades específicas das comunidades.

### Uso de Indicadores na Saúde Mental
Embora haja uma grande quantidade de dados coletados diariamente pelos serviços de saúde, incluindo os da RAPS, esses dados frequentemente não são utilizados de forma eficaz. A capacidade analítica dos governos para transformar esses dados em informações úteis é limitada, tornando a coleta de dados muitas vezes uma atividade burocrática. O uso de indicadores nas RAPS é incipiente e enfrenta desafios devido à resistência dos profissionais e problemas no fluxo de dados entre sistemas. Atualmente, não existe um conjunto abrangente de indicadores para monitorar a efetividade do sistema de saúde mental no Brasil.

### Sobre o Desafio
O Projeto Aplicado teve como objetivo elaborar o desenho de ponta a ponta das etapas de implementação de um projeto de painel de indicadores em Saúde Mental a partir de dados públicos gerados nos próprios serviços da RAPS e disponibilizados em bases abertas e anonimizadas pelo Ministério da Saúde por meio do Departamento de Informática do Sistema Único de Saúde (DATASUS), além da implementação localmente da primeira etapa desse desenho, que consistiu na extração, tratamento e carregamento local da base de registro de Procedimentos Ambulatoriais.

*******

<div id='estrutura'/>  
 
## :milky_way: Estrutura do repositório

O repositório é estruturado da seguinte forma:

```plain

├─ pos-projeto-aplicado
│  ├─ arquivos_baixados
│  ├─ arquivos_pa
│  ├─ etl
│  │  ├─ app.py
│  │  ├─ etl_local_pa.py
│  │  └─ requirements.txt
│  ├─ dockerfile.txt
│  └─ README.md
```

*******

<div id='instalacao'/> 

## 🛠️ Instalação

<div id='local'/> 
  
### Instalação via código fonte 

```sh
# clonar e acessar a raíz do repositório
$ git clone https://github.com/fernandesbruna/pos-projeto-aplicado.git
$ cd pos-projeto-aplicado

```

<div id='docker'/> 
  
### Instalação e execução locais com Docker

Execute os comandos abaixo em um terminal de linha de comando:

```sh
$ docker build -t projeto-aplicado .
$ docker run -p 8888:8888 projeto-aplicado:latest
```

Esses comandos vão construir uma cópia local da imagem do projeto aplicado e tentar executar a extração e tratamento dos dados de PA.

<div id='utilizacao'/> 

## :gear: Utilização via flask

Para utilizar o script de ETL via Flask, siga os passos:

### 1. Configuração do Ambiente
Antes de iniciar o servidor Flask, certifique-se de ter instalado todas as dependências necessárias. Isso pode ser feito utilizando o comando:
```bash
$ pip install -r requirements.txt
```

### 2. Inicialize o servidor flask localmente
Para iniciar o servidor Flask localmente, utilize o comando:
```sh
python app.py
```

### 3. Realize uma requisição à API
Para realizar uma requisição POST à API, utilize a ferramenta curl ou qualquer cliente HTTP de sua preferência. 
A requisição deve conter um payload JSON com os seguintes parâmetros:
- UF: Sigla da Unidade Federativa cujos procedimentos se pretende obter (ex: "AC");
- data: Data de início do período de tempo para o qual os dados serão obtidos, no formato YYYY-MM-DD (ex: "2023-01-01");
- diretorio: Caminho completo para a pasta onde se pretende salvar o arquivo final após o processamento.

Abaixo está um exemplo de como realizar uma requisição utilizando curl para o estado do Acre na competência de janeiro/2023:
```sh
$ curl --location --request POST 'http://0.0.0.0:8080/pa' \
    --header 'Content-Type: application/json' \
    --data '{
     "UF": "AC",
     "data": "2023-01-01",
     "diretorio": "/seu-diretorio/"
}'
```
### 4. Estrutura da Resposta
A resposta da API será um JSON contendo as seguintes informações:
```yaml
{
  status: Status da operação (ex: "OK").
  estado: Sigla da Unidade Federativa processada.
  periodo: Período de competência dos dados processados, no formato YYMM (ex: "2301").
  arquivos_origem_dbc: Lista de arquivos .dbc capturados e processados.
  local: Caminho completo do arquivo CSV gerado e salvo localmente.
}
```


*******
