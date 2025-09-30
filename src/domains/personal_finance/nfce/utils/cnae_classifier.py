"""
CNAE Classifier for automatic establishment type identification
Uses Receita Federal API to get CNAE codes and classify establishments
"""

import requests
import time
from typing import Optional, Dict, Any
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager


class CNAEClassifier:
    """
    Classifies establishments by their CNAE (National Classification of Economic Activities)
    """

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("CNAEClassifier")
        self.cache = CacheManager.get_instance()

        # Mapeamento CNAE -> Tipo de estabelecimento (baseado em CNAE 2.0 oficial)
        self.cnae_mapping = {
            # Fabricação de alimentos (seção C - Indústrias de transformação)
            "10.91": "Padaria",  # Fabricação de produtos de padaria e confeitaria
            "1091": "Padaria",
            "10911": "Padaria",
            "109110": "Padaria",
            "1091101": "Padaria",  # Fabricação de produtos de padaria e confeitaria com predominância de produção própria
            "1091102": "Confeitaria",  # Fabricação de produtos de padaria e confeitaria com predominância de revenda
            # Supermercados e comércio varejista de alimentos
            "47.11": "Supermercado",  # Comércio varejista não especializado, com predominância de produtos alimentícios
            "4711": "Supermercado",  # Formato alternativo
            "47113": "Supermercado",  # Classe
            "471130": "Supermercado",  # Grupo
            "4711301": "Hipermercado",  # Hipermercados (área > 5.000 m²)
            "4711302": "Supermercado",  # Supermercados (área 300-5.000 m²)
            "47.12": "Minimercado",  # Comércio varejista de mercadorias em geral, com predominância de produtos alimentícios
            "4712": "Minimercado",  # Formato alternativo
            "47121": "Minimercado",  # Classe
            "471210": "Minimercado",  # Grupo
            "4712100": "Minimercado",  # Minimercados, mercearias e armazéns (área < 300 m²)
            "47.13": "Loja de Departamentos",  # Lojas de departamentos ou magazines
            "4713": "Loja de Departamentos",
            "47130": "Loja de Departamentos",
            "4713001": "Loja de Departamentos",  # Lojas de departamentos ou magazines
            "4713002": "Loja de Variedades",  # Lojas de variedades
            "4713003": "Duty Free",  # Lojas duty free
            # Alimentação especializada
            "47.21": "Padaria",  # Comércio varejista de produtos alimentícios em lojas especializadas
            "4721": "Padaria",
            "47211": "Padaria",
            "472110": "Padaria",
            "4721101": "Padaria",  # Padarias e confeitarias com predominância de produção própria
            "4721102": "Confeitaria",  # Padarias e confeitarias com predominância de revenda
            "4721103": "Casa de Doces",  # Comércio varejista de doces, balas, bombons e semelhantes
            "47.22": "Açougue",  # Comércio varejista de carnes e pescados
            "4722": "Açougue",
            "47221": "Açougue",
            "472210": "Açougue",
            "4722101": "Açougue",  # Açougues
            "4722102": "Peixaria",  # Peixarias
            "4722103": "Avícola",  # Comercio varejista de carnes - açougues
            "47.23": "Comércio de Bebidas",  # Comércio varejista de bebidas
            "4723": "Comércio de Bebidas",
            "47230": "Comércio de Bebidas",
            "472300": "Comércio de Bebidas",
            "4723000": "Comércio de Bebidas",
            "47.24": "Hortifruti",  # Comércio varejista hortifrutigranjeiros
            "4724": "Hortifruti",
            "47240": "Hortifruti",
            "472400": "Hortifruti",
            "4724100": "Hortifruti",
            "47.29": "Comércio de Alimentos",  # Comércio varejista de produtos alimentícios em lojas especializadas
            "4729": "Comércio de Alimentos",
            "47296": "Comércio de Alimentos",
            "472969": "Comércio de Alimentos",
            "4729699": "Comércio de Alimentos",  # Produtos alimentícios em geral não especificados
            # Farmácias e produtos de saúde (baseado na pesquisa oficial)
            "47.71": "Farmácia",  # Comércio varejista de produtos farmacêuticos
            "4771": "Farmácia",
            "47717": "Farmácia",
            "477170": "Farmácia",
            "4771701": "Drogaria",  # Sem manipulação de fórmulas
            "4771702": "Farmácia",  # Com manipulação de fórmulas
            "4771703": "Farmácia Homeopática",  # Produtos farmacêuticos homeopáticos
            "47.72": "Produtos Médicos",  # Comércio varejista de produtos médicos e ortopédicos
            "4772": "Produtos Médicos",
            "47720": "Produtos Médicos",
            "477200": "Produtos Médicos",
            "4772000": "Produtos Médicos",
            # Postos de combustível
            "47.30": "Posto de Combustível",  # Comércio varejista de combustíveis para veículos automotores
            "4730": "Posto de Combustível",
            "47301": "Posto de Combustível",
            "473010": "Posto de Combustível",
            "4730101": "Posto de Combustível",  # Postos de combustíveis e serviços para veículos
            "4730102": "Loja de Conveniência",  # Lojas de conveniência
            "47.31": "Posto de Combustível",  # Comércio varejista de combustíveis para veículos automotores
            "4731": "Posto de Combustível",
            "47318": "Posto de Combustível",
            "473180": "Posto de Combustível",
            "4731800": "Posto de Combustível",  # Comércio varejista de combustíveis para veículos automotores
            # Restaurantes e alimentação (baseado na pesquisa)
            "56.11": "Restaurante",  # Restaurantes e outros serviços de alimentação e bebidas
            "5611": "Restaurante",
            "56112": "Restaurante",
            "561120": "Restaurante",
            "5611201": "Restaurante",  # Restaurantes e similares
            "5611202": "Bares",  # Bares e outros estabelecimentos especializados em servir bebidas
            "5611203": "Lanchonete",  # Lanchonetes, casas de chá, de sucos e similares
            "5611204": "Cantina",  # Cantinas - serviços de alimentação privativos
            "5611205": "Bufê",  # Bufês
            "56.12": "Serviços de Alimentação",  # Serviços ambulantes de alimentação
            "5612": "Serviços de Alimentação",
            "56121": "Serviços de Alimentação",
            "561210": "Serviços de Alimentação",
            "5612100": "Serviços de Alimentação",  # Serviços ambulantes de alimentação
            "56.20": "Fornecimento de Alimentos",  # Serviços de catering e outros serviços de alimentação
            "5620": "Fornecimento de Alimentos",
            "56201": "Fornecimento de Alimentos",
            "562010": "Fornecimento de Alimentos",
            "5620101": "Fornecimento de Alimentos",  # Serviços de alimentação para eventos
            "5620102": "Cantina Industrial",  # Cantinas industriais, bufês industriais
            "5620103": "Catering",  # Serviços de alimentação sob contrato
            "5620104": "Delivery",  # Fornecimento de alimentos preparados preponderantemente para consumo domiciliar
            # Atacado e distribuição (baseado na pesquisa)
            "46.39": "Atacado",  # Comércio atacadista de produtos alimentícios em geral
            "4639": "Atacado",
            "46397": "Atacado",
            "463970": "Atacado",
            "4639701": "Distribuidora",  # Comércio atacadista de produtos alimentícios em geral
            "4639702": "Distribuidora",  # Com atividade de fracionamento e acondicionamento
            "46.91": "Atacado",  # Comércio atacadista de mercadorias em geral
            "4691": "Atacado",
            "46915": "Atacado",
            "469150": "Atacado",
            "4691500": "Atacado",  # Comércio atacadista de mercadorias em geral, com predominância de produtos alimentícios
            # Vestuário e acessórios
            "47.81": "Roupas e Acessórios",  # Comércio varejista de artigos do vestuário e acessórios
            "4781": "Roupas e Acessórios",
            "47811": "Roupas e Acessórios",
            "478110": "Roupas e Acessórios",
            "4781100": "Roupas e Acessórios",
            "47.82": "Calçados",  # Comércio varejista de calçados e artigos de viagem
            "4782": "Calçados",
            "47821": "Calçados",
            "478210": "Calçados",
            "4782100": "Calçados",
            # Móveis e decoração
            "47.51": "Móveis",  # Comércio varejista de tecidos, artigos de armarinho, vestuário e calçados
            "4751": "Móveis",
            "47511": "Móveis",
            "475110": "Móveis",
            "4751100": "Móveis",
            "47.52": "Ferragens",  # Comércio varejista de ferragens, madeira e materiais de construção
            "4752": "Material de Construção",
            "47521": "Material de Construção",
            "475210": "Material de Construção",
            "4752100": "Material de Construção",
            "47.53": "Eletrodomésticos",  # Comércio varejista de eletrodomésticos e equipamentos de áudio e vídeo
            "4753": "Eletrodomésticos",
            "47531": "Eletrodomésticos",
            "475310": "Eletrodomésticos",
            "4753100": "Eletrodomésticos",
            "47.59": "Móveis e Decoração",  # Comércio varejista de móveis, artigos de iluminação e outros artigos de residência
            "4759": "Móveis e Decoração",
            "47591": "Móveis e Decoração",
            "475910": "Móveis e Decoração",
            "4759100": "Móveis e Decoração",
            # Livrarias e papelarias
            "47.61": "Livraria",  # Comércio varejista de livros, jornais, revistas e papelaria
            "4761": "Livraria",
            "47611": "Livraria",
            "476110": "Livraria",
            "4761100": "Livraria",
            "47.62": "Papelaria",  # Comércio varejista de discos, CDs, DVDs e fitas
            "4762": "CDs e DVDs",
            "47621": "CDs e DVDs",
            "476210": "CDs e DVDs",
            "4762100": "CDs e DVDs",
            # Outros comércios
            "47.90": "Outros Comércios",  # Comércio varejista de artigos usados
            "4790": "Outros Comércios",
            "47901": "Brechó",
            "479010": "Brechó",
            "4790100": "Brechó",
            # Reparos e manutenção
            "95.11": "Reparação Eletrônicos",  # Reparação e manutenção de equipamentos de informática e comunicação
            "9511": "Reparação Eletrônicos",
            "95111": "Reparação Eletrônicos",
            "951110": "Reparação Eletrônicos",
            "9511100": "Reparação Eletrônicos",
            "95.12": "Reparação Eletrodomésticos",  # Reparação e manutenção de equipamentos eletroeletrônicos de uso pessoal e doméstico
            "9512": "Reparação Eletrodomésticos",
            "95121": "Reparação Eletrodomésticos",
            "951210": "Reparação Eletrodomésticos",
            "9512100": "Reparação Eletrodomésticos",
            "95.21": "Reparação Calçados",  # Reparação e manutenção de objetos pessoais e domésticos
            "9521": "Reparação Calçados",
            "95211": "Reparação Calçados",
            "952110": "Reparação Calçados",
            "9521100": "Reparação Calçados",
            "95.29": "Outros Reparos",  # Manutenção e reparação de outros objetos pessoais e domésticos
            "9529": "Outros Reparos",
            "95291": "Outros Reparos",
            "952910": "Outros Reparos",
            "9529100": "Outros Reparos",
        }

    def get_establishment_info(self, cnpj: str) -> Optional[Dict[str, Any]]:
        """
        Consulta informações do estabelecimento via API da Receita Federal

        Args:
            cnpj: CNPJ do estabelecimento (apenas números)

        Returns:
            Dict com informações do estabelecimento ou None se erro
        """
        try:
            # Limpar CNPJ (apenas números) e normalizar
            clean_cnpj = "".join(filter(str.isdigit, cnpj))

            # Normalizar para 14 dígitos
            if len(clean_cnpj) == 14:
                pass  # CNPJ correto
            elif len(clean_cnpj) < 14:
                # Preencher com zeros à esquerda
                clean_cnpj = clean_cnpj.zfill(14)
                self.logger.info(f"CNPJ padded: {cnpj} -> {clean_cnpj}")
            else:
                # Se tem mais de 14 dígitos, pegar apenas os primeiros 14
                clean_cnpj = clean_cnpj[:14]
                self.logger.warning(f"CNPJ truncated: {cnpj} -> {clean_cnpj}")

            if len(clean_cnpj) != 14:
                self.logger.error(
                    f"CNPJ inválido após normalização: {cnpj} -> {clean_cnpj}"
                )
                return None

            # Verificar cache primeiro
            cache_key = f"cnpj_{clean_cnpj}"
            cached_data = self.cache.load(
                cache_key, expiration_minutes=1440
            )  # 24 horas

            if cached_data:
                self.logger.info(f"Using cached CNAE data for CNPJ: {clean_cnpj}")
                return cached_data

            # Consultar API da Receita Federal com retry
            url = f"https://www.receitaws.com.br/v1/cnpj/{clean_cnpj}"

            # Rate limiting - aguardar 1 segundo entre requests
            time.sleep(1)

            # Tentar até 3 vezes com delays progressivos
            max_retries = 3
            data = None

            for attempt in range(max_retries):
                try:
                    response = requests.get(
                        url, timeout=45
                    )  # Aumentar timeout para 45s
                    response.raise_for_status()

                    data = response.json()

                    # Verificar se a resposta tem dados válidos
                    if not data or not isinstance(data, dict):
                        raise ValueError(f"Invalid response format: {data}")

                    # Se chegou aqui, foi sucesso
                    break

                except (requests.exceptions.RequestException, ValueError) as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 3  # 3, 6, 9 segundos
                        self.logger.warning(
                            f"API request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        # Última tentativa falhou
                        self.logger.error(
                            f"API request failed after {max_retries} attempts for CNPJ {clean_cnpj}: {e}"
                        )
                        return None

            if not data:
                self.logger.error(f"No data received from API for CNPJ {clean_cnpj}")
                return None

            # Verificar se retornou erro
            if data.get("status") == "ERROR":
                self.logger.error(
                    f"API error for CNPJ {clean_cnpj}: {data.get('message', 'Unknown error')}"
                )
                return None

            # Extrair informações relevantes
            result = {
                "cnpj": data.get("cnpj"),
                "business_name": data.get("nome"),
                "trade_name": data.get("fantasia"),
                "cnae_principal": data.get("atividade_principal", [{}])[0].get("code")
                if data.get("atividade_principal")
                else None,
                "cnae_description": data.get("atividade_principal", [{}])[0].get("text")
                if data.get("atividade_principal")
                else None,
                "address": self._format_address(data),
                "city": data.get("municipio"),
                "state": data.get("uf"),
                "status": data.get("situacao"),
                "establishment_type": None,  # Will be filled by classify_establishment
            }

            # Classificar tipo de estabelecimento
            if result["cnae_principal"]:
                result["establishment_type"] = self.classify_establishment(
                    result["cnae_principal"]
                )

            # Salvar no cache
            self.cache.save(cache_key, result)

            self.logger.info(
                f"Successfully retrieved CNAE data for {clean_cnpj}: {result['establishment_type']}"
            )
            return result

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching CNAE for {cnpj}: {e}")
            # Salvar erro no cache de sessão para evitar retentar
            return None
        except Exception as e:
            self.logger.error(f"Error fetching CNAE data for {cnpj}: {e}")
            return None

    def classify_establishment(self, cnae_code: str) -> str:
        """
        Classifica o tipo de estabelecimento baseado no código CNAE

        Args:
            cnae_code: Código CNAE (ex: "47.11-3-02")

        Returns:
            Tipo de estabelecimento ou "Outros" se não encontrado
        """
        if not cnae_code:
            return "Outros"

        # Pegar apenas os primeiros 5 caracteres (ex: "47.11")
        cnae_prefix = cnae_code[:5]

        establishment_type = self.cnae_mapping.get(cnae_prefix, "Outros")

        self.logger.debug(f"CNAE {cnae_code} -> Type: {establishment_type}")
        return establishment_type

    def _format_address(self, data: dict) -> Optional[str]:
        """
        Formata o endereço a partir dos dados da API
        """
        try:
            parts = []

            if data.get("logradouro"):
                parts.append(data["logradouro"])

            if data.get("numero"):
                parts.append(data["numero"])

            if data.get("complemento"):
                parts.append(data["complemento"])

            if data.get("bairro"):
                parts.append(data["bairro"])

            if data.get("cep"):
                parts.append(f"CEP: {data['cep']}")

            return ", ".join(parts) if parts else None

        except Exception as e:
            self.logger.warning(f"Error formatting address: {e}")
            return None

    def get_establishment_type_summary(self) -> Dict[str, int]:
        """
        Retorna um resumo dos tipos de estabelecimento disponíveis
        """
        type_counts = {}
        for establishment_type in self.cnae_mapping.values():
            type_counts[establishment_type] = type_counts.get(establishment_type, 0) + 1

        return type_counts
