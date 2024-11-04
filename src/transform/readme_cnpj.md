
### Ordem de Execução e Descrição dos Métodos

#### Arquivo: `transform_cnpj.py`

1. **Método: `transform_cnpj`**
   - **Descrição**: Método principal que orquestra a transformação dos dados de CNPJ. Define parâmetros básicos, executa a transformação de especificações, carrega o cache adequado e aplica o delta entre o DataMesh e o cache.
   - **Chama os métodos**:
      - `transform_spec` (do `transform_helpers.py`) - Passo 2
      - `cache_cnpj` (do `cache_handler.py`) - Passo 5
      - `transform_delta` (do `transform_helpers.py`) - Passo 7

#### Arquivo: `transform_helpers.py`

2. **Método: `transform_spec`**
   - **Descrição**: Realiza a transformação das especificações do DataMesh para calcular as diferenças entre as partições com base no tipo de processo (full ou delta).
   - **Chama os métodos**:
      - `get_partition_dates` - Passo 3
      - `get_distinct_partition` - Passos 4, 6
      - `get_insert_df` e `get_update_df` (caso de processo incremental) - Passo 6

3. **Método: `get_partition_dates`**
   - **Descrição**: Obtém as datas de partição para a transformação, determinando as datas mais recentes e anteriores conforme o parâmetro passado.
   - **Chama**: `get_partition_date` (uma função auxiliar externa).

4. **Método: `get_distinct_partition`**
   - **Descrição**: Seleciona e filtra a partição de dados correspondente ao último ou penúltimo período especificado, aplicando filtro por filial "0001" para CNPJ9.

5. **Retorno ao `transform_spec`**
   - **Descrição**: Dependendo do tipo de processo, o método retorna a partição mais recente (para full) ou executa o delta entre partições.

6. **Métodos: `get_insert_df` e `get_update_df`**
   - **Descrição**: Para processos incrementais, calcula registros de inserção e atualização entre partições (delta), retornando o conjunto unificado desses registros para a transformação.
   
7. **Método: `transform_delta`**
   - **Descrição**: Aplica o delta entre as especificações processadas e o cache, retornando os dados com as informações atualizadas.

#### Arquivo: `cache_handler.py`

8. **Método: `cache_cnpj`**
   - **Descrição**: Determina qual cache específico de CNPJ deve ser carregado (`CNPJ9` ou `CNPJ14`).
   - **Chama os métodos**:
      - `cache_cnpj9` ou `cache_cnpj14` (de acordo com o tipo de documento) - Passo 9 ou 10

9. **Método: `cache_cnpj9`**
   - **Descrição**: Carrega o cache de CNPJ9 (implementação omitida).

10. **Método: `cache_cnpj14`**
    - **Descrição**: Carrega o cache de CNPJ14 (implementação omitida).

### Resumo da Ordem de Execução para o Fluxograma

1. `transform_cnpj` (transform_cnpj.py)
2. `transform_spec` (transform_helpers.py)
3. `get_partition_dates` (transform_helpers.py)
4. `get_distinct_partition` (transform_helpers.py)
5. `cache_cnpj` (cache_handler.py)
6. `get_insert_df` e `get_update_df` (transform_helpers.py) - se necessário
7. `transform_delta` (transform_helpers.py)
8. `cache_cnpj9` ou `cache_cnpj14` (cache_handler.py) - de acordo com o tipo

Essa sequência permitirá uma visão clara do fluxo de dados e das etapas de transformação para criação do fluxograma.