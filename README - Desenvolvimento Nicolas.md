# Desafio Engenheiro de Dados
Este projeto contempla o desafio passado pela empresa RADIX para mim, Nicolas Marcos, para vaga de Engenheiro de Dados.

## Sobre o Desafio

O desafio consistia em realizar um processo ETL utilizando a ferramenta Apache Airflow, sobre dados de Covid19 csv presentes em 3 arquivos diferentes presentes em um filesystem local. O desafio consistia em 2 etapas, sendo a primeira consumir dados de uma camada raw/bronze e realizar sua carga em uma camada silver/trusted, realizando algumas transformações e os carregando em parquet particionado. Na sequência, realizando um processo ETL de uma camada silver/trusted para uma camada refined/gold com a visão de uma tabela agregada na granularidade de média movel por data e país.

## Sobre o processo de desenvolvimento

Uma vez que não dispunha de conhecimento na ferramenta Apache Airflow, inicialmente busquei conhecimento na ferramenta. Realizei 3 cursos e neste momento estou prosseguindo em um quarto. Ao contrário de alguns cursos que montavem cenários muito específicos para a ferramentas, a instalei em uma VM que preparei DO ZERO com um ecossistema Hadoop instalado em OS CentOS, já pensando em aplicação em um cenário de Big Data.

Inicialmente, realizei uma série de vídeos e tutoriais até adquirir conhecimento básico na ferramenta, o que foi facilitado por minha experiência em outras ferramentas de orquestração e ETL. Após o entendimento dos principais conceitos, como DAG, Hooks, Operators e outros, prossegui no projeto e o realizei criando em um único código Python DAGs com tasks usando PythonOperator com e conexões hardcode.

Uma vez que dispunha de um MVP do projeto funcionando e atendendo os requisitos passados, busquei empregar boas-práticas e conhecimentos adquiridos de outras ferramentas, como isolar do código strings de conexão e paths, separar o código em itens de acordo com o objetivo, entre outros. Em tempo, pesquisei também boas-práticas de airflow que pudessem ser empregues. Deste modo, realizei os seguintes ajustes:

1) Ainda no cenário do código todo contido em uma DAG, separei import de libs específicas para as tasks nas respectivas funções. É dito dentre boas-práticas que pode vir a deixar o processamento mais lento trazer todas as libs necessárias de maneira global na dag, em comparação as deixar nas funções de tasks específicas.

2) Separei as strings de conexões através de variáveis de ambiente. Por se tratar de filepaths onde não há requisito (portanto, entendo não ser desejado) monitorar modificações nos arquivos, não foi utilizado filesensors que captam mudanças automáticas no arquivo. Do contrário, esta poderia ser uma alternativa. 

3) Pensando em evolução futura, foram criadas pastas dentro do escopo do projeto dentro do diretório dags/src para hooks, notebooks, operators, tasks e scripts. Tais componentes se desejados futuramente devem ser organizados em tais diretórios.

4) Buscando maior facilidade de manutenção, houve componentização dos processos de carga em tasks que foram disponibilizadas na pasta de tasks. Foram criados os scripts de carga "carga_refined.py" e "carga_trusted.py" e utilizados decorators para que seguissem os comportamentos de tasks. Então, estes foram importados como módulos dentro do código da dag "covid19.py"

### Observações:

Devido a natureza dos dados dos datasets, haviam registros com valores nulos. Ao realizar processo de union entre as bases, tal cenário gerava cartesiano, pois combinava registros de valores nulos. Para tal, tomou-se decisão ao longo do projeto de preencher valores faltantes como "-1" (com sting para campos categóricos e long (em acordo com requisito) para campos numéricos), de modo que este atuou como uma flag e permitiu a devida união dos datasets. 

Verifica-se ainda que os datasets originais de confirmados e mortes possuem 131175 registros (após os ajustes para transformar as datas de colunas), enquanto que o dataset de recuperados após tais ajustes 124020 registros. Ao realizar o join entre as bases para construir os indicadores de soma e com isto posteriormente as médias-móveis e partindo da premissa que todos os datasets possuem datas comuns entre si, esperar-se-ia que o dataset final dispusesse de 131175 registros. Contudo, percebeu-se que no dataset de recuperados há registros com campos nulos não comuns nos outros datasets. Por exemplo, o país Canada nos datasets de confirmados e mortes possuem seus estados preenchidos, porém isto não ocorre no dataset de recuperados. Este tipo de cenário acaba por gerar um "cartesiano" entre as bases, pois onde os registros de confirmados e mortes não tiverem correspondência exata nos recuperados, haverão mais linhas para aquele país, todavia com os indicadores de confirmados e mortes virão nulos enquanto que os de recuperados virão preenchidos quando forem registros que existem em recuperados, mas não nos outros; e haverão registros com as quantidades de recuperados nulas nas linhas advindas da união de confirmados e mortes, mas sem correspondência em recuperados. Não ocorre cenário prejudicial de duplicidade dos quantitativos. Cenários dos testes realizados podem ser encontrados em: dags\src\notebooks\testes


## Sobre a execução

Para executar este código, basta seguir os passos descritos no README.md inicial presente nesta pasta e importar o arquivo "variables.json" presente na pasta do projeto. Dependendo do filesystem de seu host, modificações nos valores das strings de conexão podem ser necessários.

## Sobre Feedback Geral

O desafio foi muito divertido e agregou muito meus conhecimentos. Já estava em meu roadmap aprender a ferramenta, mas devido ao desafio tive de antecipar esta etapa e valeu a pena. Tenho certeza que há pontos a melhorar, porém dentro do prazo que tive busquei entregar o melhor que pude e fiquei muito contente com o resultado. Caso tenha a honra de ser escolhido para compor o time, será um prazer trocar minhas ideias e experiências com este time de peso e também aprender o máximo que puder. Em linhas gerais, agradeço a oportunidade e diversão que tive. 