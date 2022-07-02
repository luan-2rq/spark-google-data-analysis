## Descrição
Análise de traços de execução do Google Borg feita usando Apache Spark. Google Borg é um sistema responsavel por gerenciar os clusters da Google.

## Pré-Requisitos - Linux

Como pré-requisitos para rodar o projeto é necessário possuir o Python e o Java instalados. Idealmente a versão do Java baixada deve ser a 11 e a versão do python atualizada. Caso necessário, as variáveis de ambiente do java e python devem ser configuradas de acordo e predefinidas na variável path, para que assim os comandos do java e python sejam reconhecidos diretamente em qualquer pasta no terminal do Linux.

## Instalações - Linux

* Dependências 

Utilizamos o gerenciador de pacotes do Python, o pip, para instalar todas as dependências que o projeto funcional requer para ser executado. Dessa forma, após descompactar o arquivo submetido e estar na pasta raiz do projeto, execute o seguinte comando: pip install -r requirements.txt para instalar os pacotes antes de executar de fato o programa. Caso você tenha o Python3 instalado na sua máquina, mas o que é utilizado por padrão seja o Python2.7 (comum de ocorrer em sistemas UNIX-based), você pode tentar os seguintes comandos alternativos:
pip3 install -r requirements.txt
python3 -m pip install -r requirements.txt
Caso mesmo assim você ainda não consiga rodar o pip, recomendamos que acesse essa página sobre ambiente virtual utilizando pip e tente novamente.

* Spark - Linux

Será necessário baixar o Spark para assim conseguir utilizar a biblioteca pyspark. O spark pode ser baixado através da seguinte página. Utilizamos da versão 3.2.1 (Jan 26 2022) do spark, porém a versão mais recente deve funcionar também.
Após descompactar o Spark, será necessário configurar o caminho do spark na variável de ambiente path para que assim ele seja reconhecido pelo pyspark.

## Execução 

Foram criados arquivos .py para cada uma das questões perguntadas pelo enunciado do EP. Os arquivos seguem a seguinte nomenclatura e se referem às seguintes questões:

| Nome  |                                                                    Questão                                                                   |
|-------|:--------------------------------------------------------------------------------------------------------------------------------------------:|
| q1.py |                         “Como é a requisição de recursos computacionais (memória e CPU) do cluster durante o tempo? ”                        |
| q2.py | “As diversas categorias de jobs possuem características diferentes (requisição de recursos computacionais, frequência de submissão, etc.)? ” |
| q3.py |                                                   “Quantos jobs são submetidos por hora? ”                                                   |
| q4.py |                                                  “Quantas tarefas são submetidas por hora?”                                                  |
| q5.py |                                “Quanto tempo demora para a primeira tarefa de um job começar a ser executada?”                               |
| q6.py |                                         Qual a frequência de cada tipo de job para os eventos de job?                                        |
| q7.py |                                       Qual a frequência de cada tipo de job para os eventos de tarefas?                                      |

As questões 6 e 7 não estavam presentes no enunciado do EP, porém foram adicionadas para a nossa análise dos dados disponibilizados.

Estando com todas as dependências e a instalação do python, java e spark feita,  a execução pode ser feita a partir do seguinte comando no terminal: “python caminho-do-arquivo”, sendo caminho-do-arquivo o caminho do arquivo .py a ser executado. Antes da execução desses arquivos é importante que o caminho para os dados seja configurado corretamente, nas linhas iniciais de cada um dos arquivos o caminho dos dados é configurado, portanto este caminho deve ser configurado de acordo com o caminho dos dados em sua máquina.  Ao executar um desses arquivos serão imprimidos na tela os resultados obtidos para a questão a qual o arquivo se refere, sendo gráficos ou objetivamente valores referentes ao resultado.
