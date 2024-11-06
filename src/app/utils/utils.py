def get_schema(tabela: str):
    if tabela == 'cnpj9':
        return _schema_cnpj9()
    else:
        raise Exception(f'Schema não encontrado para a tabela {tabela}')


def _schema_cnpj9():
    schema = {
        'tabela': 'cnpj9',
        'spec': {
            'cnpj_basico': 'num_cpfcnpj',
            'cnpj_ordem': 'num_cpfcnpj',
            'cnpj_dv': 'num_cpfcnpj',
            'cnpj': 'num_cpfcnpj',
            'data': 'num_cpfcnpj',
        },

        'cache_dynamics': {
            'cnpj_basico': 'num_cpfcnpj',
            'cnpj_ordem': 'num_cpfcnpj',
            'cnpj_dv': 'num_cpfcnpj',
            'cnpj': 'num_cpfcnpj',
            'data': 'num_cpfcnpj',
        },

        'delta_cnpj9': {
            'cnpj_basico': 'num_cpfcnpj',
            'cnpj_ordem': 'num_cpfcnpj',
            'cnpj_dv': 'num_cpfcnpj',
            'cnpj': 'num_cpfcnpj',
            'data': 'num_cpfcnpj',
        }
    }

    return schema
