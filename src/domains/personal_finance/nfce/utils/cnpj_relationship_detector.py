#!/usr/bin/env python3
"""
CNPJ Relationship Detector - Identifica filiais e empresas relacionadas
"""

import re
import requests
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager


@dataclass
class CompanyInfo:
    """Informações da empresa extraídas do CNPJ"""

    cnpj: str
    cnpj_root: str  # Primeiros 8 dígitos
    company_name: str
    trade_name: Optional[str]
    is_branch: bool
    main_cnpj: Optional[str]  # CNPJ da matriz se for filial
    branch_number: str  # Últimos 4 dígitos antes do DV


class CNPJRelationshipDetector:
    """Detecta relacionamentos entre CNPJs (matriz/filial)"""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("CNPJRelationshipDetector")
        self.cache = CacheManager.get_instance()
        self.rate_limit_delay = 1.0  # 1 segundo entre requests
        self.last_request_time = 0

    def extract_cnpj_components(self, cnpj: str) -> Dict[str, str]:
        """Extrai componentes do CNPJ para análise"""
        # Remove formatação
        clean_cnpj = re.sub(r"[^\d]", "", cnpj)

        if len(clean_cnpj) != 14:
            raise ValueError(f"CNPJ inválido: {cnpj}")

        return {
            "full": clean_cnpj,
            "root": clean_cnpj[:8],  # Identifica a empresa
            "branch": clean_cnpj[8:12],  # Número da filial (0001 = matriz)
            "check_digits": clean_cnpj[12:14],
        }

    def is_same_company_by_root(self, cnpj1: str, cnpj2: str) -> bool:
        """Verifica se dois CNPJs pertencem à mesma empresa (mesmo root)"""
        try:
            components1 = self.extract_cnpj_components(cnpj1)
            components2 = self.extract_cnpj_components(cnpj2)
            return components1["root"] == components2["root"]
        except ValueError:
            return False

    def is_likely_branch(self, cnpj: str) -> bool:
        """Verifica se CNPJ é provavelmente uma filial (branch != 0001)"""
        try:
            components = self.extract_cnpj_components(cnpj)
            return components["branch"] != "0001"
        except ValueError:
            return False

    def get_company_info_brasil_api(self, cnpj: str) -> Optional[CompanyInfo]:
        """Consulta informações da empresa via Brasil API"""
        cache_key = f"cnpj_info_{cnpj}"
        cached_info = self.cache.load(
            cache_key, expiration_minutes=1440
        )  # Cache por 24h

        if cached_info:
            self.logger.debug(f"Using cached CNPJ info for {cnpj}")
            return CompanyInfo(**cached_info)

        try:
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                time.sleep(self.rate_limit_delay - time_since_last)

            self.last_request_time = time.time()

            clean_cnpj = re.sub(r"[^\d]", "", cnpj)
            url = f"https://brasilapi.com.br/api/cnpj/v1/{clean_cnpj}"

            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            components = self.extract_cnpj_components(cnpj)

            company_info = CompanyInfo(
                cnpj=clean_cnpj,
                cnpj_root=components["root"],
                company_name=data.get("razao_social", ""),
                trade_name=data.get("nome_fantasia"),
                is_branch=components["branch"] != "0001",
                main_cnpj=f"{components['root']}0001{components['check_digits']}"
                if components["branch"] != "0001"
                else None,
                branch_number=components["branch"],
            )

            # Cache the result
            cache_data = {
                "cnpj": company_info.cnpj,
                "cnpj_root": company_info.cnpj_root,
                "company_name": company_info.company_name,
                "trade_name": company_info.trade_name,
                "is_branch": company_info.is_branch,
                "main_cnpj": company_info.main_cnpj,
                "branch_number": company_info.branch_number,
            }
            self.cache.save(cache_key, cache_data)

            self.logger.info(
                f"Retrieved CNPJ info for {cnpj}: {company_info.company_name}"
            )
            return company_info

        except Exception as e:
            self.logger.error(f"Error fetching CNPJ info for {cnpj}: {e}")
            return None

    def find_related_establishments(self, cnpj_list: List[str]) -> Dict[str, List[str]]:
        """Agrupa CNPJs por empresa (mesmo root)"""
        company_groups: Dict[str, List[str]] = {}

        for cnpj in cnpj_list:
            try:
                components = self.extract_cnpj_components(cnpj)
                root = components["root"]

                if root not in company_groups:
                    company_groups[root] = []

                company_groups[root].append(cnpj)

            except ValueError as e:
                self.logger.warning(f"Invalid CNPJ {cnpj}: {e}")
                continue

        # Filter out single-establishment companies
        related_groups = {
            root: cnpjs for root, cnpjs in company_groups.items() if len(cnpjs) > 1
        }

        self.logger.info(
            f"Found {len(related_groups)} companies with multiple establishments"
        )
        return related_groups

    def analyze_establishment_relationships(
        self, cnpj_list: List[str]
    ) -> Dict[str, Any]:
        """Análise completa de relacionamentos entre estabelecimentos"""

        self.logger.info(f"Analyzing relationships for {len(cnpj_list)} establishments")

        # Encontrar grupos relacionados
        related_groups = self.find_related_establishments(cnpj_list)

        # Obter informações detalhadas dos CNPJs relacionados
        detailed_groups: Dict[str, Any] = {}
        total_branches = 0

        for root, cnpj_group in related_groups.items():
            detailed_groups[root] = {
                "company_name": None,
                "establishments": [],
                "branch_count": len(cnpj_group),
            }

            total_branches += len(cnpj_group)

            for cnpj in cnpj_group:
                company_info = self.get_company_info_brasil_api(cnpj)

                establishment_data = {
                    "cnpj": cnpj,
                    "branch_number": self.extract_cnpj_components(cnpj)["branch"],
                    "is_main": self.extract_cnpj_components(cnpj)["branch"] == "0001",
                }

                if company_info:
                    establishment_data.update(
                        {
                            "company_name": company_info.company_name,
                            "trade_name": company_info.trade_name,
                        }
                    )

                    if not detailed_groups[root]["company_name"]:
                        detailed_groups[root]["company_name"] = (
                            company_info.company_name
                        )

                detailed_groups[root]["establishments"].append(establishment_data)

        return {
            "total_companies_with_branches": len(detailed_groups),
            "total_branch_establishments": total_branches,
            "companies": detailed_groups,
            "single_establishments": len(cnpj_list) - total_branches,
        }

    def calculate_similarity_bonus(self, cnpj1: str, cnpj2: str) -> float:
        """Calcula bonus de similaridade baseado no relacionamento entre CNPJs"""

        if self.is_same_company_by_root(cnpj1, cnpj2):
            # Mesmo grupo empresarial = bonus alto
            return 0.15

        # Verificar se empresas têm nomes similares
        info1 = self.get_company_info_brasil_api(cnpj1)
        info2 = self.get_company_info_brasil_api(cnpj2)

        if info1 and info2:
            name1 = info1.company_name.upper()
            name2 = info2.company_name.upper()

            # Bonus menor para empresas com nomes similares
            common_words = set(name1.split()) & set(name2.split())
            if len(common_words) >= 2:
                return 0.05

        return 0.0  # Sem bonus
