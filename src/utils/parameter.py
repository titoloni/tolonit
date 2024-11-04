# src/utils/parameter.py
import sys
import json

PATH = "utils//default_parameters.json"

# Para executar: python3 app/main.py --PARAM "[{\"base\":\"CNPJ9\",\"type_process\":\"delta\",\"last_partition_date\":null,\"previous_partition_date\":null,\"is_to_process\":true}]"


class ParameterClass:
    def __init__(self):
        self._json_parameters = self._load_parameters()

    def _load_parameters(self):
        """Carrega parâmetros de linha de comando ou de um arquivo JSON."""
        print(sys.argv)
        if "--PARAM" in sys.argv:
            param_args = sys.argv[sys.argv.index("--PARAM") + 1]
            return json.loads(param_args)
        else:
            with open(PATH, "r") as file:
                return json.load(file)

    def get_all_partition_dates(self):
        """Retorna um conjunto de todas as datas de partição."""
        return {
            item["last_partition_date"]
            for item in self._json_parameters
            if item["last_partition_date"] not in (None, 0)
        }.union(
            {
                item["previous_partition_date"]
                for item in self._json_parameters
                if item["previous_partition_date"] not in (None, 0)
            }
        )

    def _get_param_value(self, base, key, default=0):
        """Retorna o valor de um parâmetro específico ou um valor padrão."""
        for item in self._json_parameters:
            if item["base"] == base:
                return item.get(key, default)
        return default

    def is_to_process(self, base):
        """Verifica se deve processar a base específica."""
        return self._get_param_value(base, "is_to_process", False)

    def get_type_process(self, base):
        """Retorna o tipo de processamento para a base específica."""
        return self._get_param_value(base, "type_process", "delta")

    def get_last_base_date(self, base):
        """Retorna a última data de base para a base específica."""
        return self._get_param_value(base, "last_partition_date", 0)

    def get_previous_base_date(self, base):
        """Retorna a data da base anterior para a base específica."""
        return self._get_param_value(base, "previous_partition_date", 0)

    def get_bases(self):
        """Retorna uma lista de todas as bases disponíveis."""
        return [item["base"] for item in self._json_parameters]
