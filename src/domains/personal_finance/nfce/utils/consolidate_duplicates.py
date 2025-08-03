#!/usr/bin/env python3
"""
Script para consolidar estabelecimentos duplicados no banco de dados NFCe
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from utils.logging.logging_manager import LogManager
from domains.personal_finance.nfce.database.nfce_database_manager import NFCeDatabaseManager


def normalize_cnpj(cnpj: str) -> str:
    """Normalizar CNPJ removendo formatação"""
    if not cnpj:
        return ""
    
    # Remove tudo que não for dígito
    clean = ''.join(filter(str.isdigit, cnpj))
    
    # Normalizar para 14 dígitos
    if len(clean) == 14:
        return clean
    elif len(clean) < 14:
        return clean.zfill(14)
    elif len(clean) > 14:
        return clean[:14]
    else:
        return clean


def consolidate_establishments():
    """Consolidar estabelecimentos duplicados"""
    logger = LogManager.get_instance().get_logger("ConsolidateDuplicates")
    
    try:
        # Inicializar database manager
        db_manager = NFCeDatabaseManager()
        conn = db_manager.db_manager.get_connection("nfce_db")
        
        logger.info("Starting establishments consolidation")
        
        # Buscar todos os estabelecimentos
        establishments = conn.execute("""
            SELECT id, cnpj, business_name, establishment_type, address, city, state, cnae_code, created_at
            FROM establishments 
            ORDER BY cnpj, created_at
        """).fetchall()
        
        # Agrupar por CNPJ normalizado
        cnpj_groups = {}
        for est in establishments:
            normalized_cnpj = normalize_cnpj(est[1])  # est[1] é o CNPJ
            
            if normalized_cnpj not in cnpj_groups:
                cnpj_groups[normalized_cnpj] = []
            cnpj_groups[normalized_cnpj].append(est)
        
        # Processar grupos com duplicatas
        consolidated_count = 0
        deleted_count = 0
        
        for normalized_cnpj, group in cnpj_groups.items():
            if len(group) > 1:
                logger.info(f"Found {len(group)} duplicates for CNPJ {normalized_cnpj}")
                
                # Escolher o melhor registro (mais completo ou mais recente)
                best_record = choose_best_record(group)
                others = [r for r in group if r[0] != best_record[0]]  # r[0] é o ID
                
                logger.info(f"Keeping record {best_record[0]} ({best_record[2]}) for CNPJ {normalized_cnpj}")
                
                # Atualizar o melhor registro com CNPJ normalizado
                conn.execute("""
                    UPDATE establishments 
                    SET cnpj = ?
                    WHERE id = ?
                """, [normalized_cnpj, best_record[0]])
                
                # Atualizar referências nas outras tabelas
                for other in others:
                    # Atualizar invoices que referenciam os estabelecimentos duplicados
                    invoice_updates = conn.execute("""
                        UPDATE invoices 
                        SET issuer_cnpj = ?
                        WHERE issuer_cnpj = ?
                    """, [normalized_cnpj, other[1]]).rowcount
                    
                    if invoice_updates > 0:
                        logger.info(f"Updated {invoice_updates} invoice references from {other[1]} to {normalized_cnpj}")
                    
                    # Atualizar products que referenciam os estabelecimentos duplicados
                    product_updates = conn.execute("""
                        UPDATE products 
                        SET establishment_id = ?
                        WHERE establishment_id = ?
                    """, [best_record[0], other[0]]).rowcount
                    
                    if product_updates > 0:
                        logger.info(f"Updated {product_updates} product references from {other[0]} to {best_record[0]}")
                    
                    # Deletar estabelecimento duplicado
                    conn.execute("DELETE FROM establishments WHERE id = ?", [other[0]])
                    deleted_count += 1
                    logger.info(f"Deleted duplicate establishment {other[0]} ({other[2]})")
                
                consolidated_count += 1
        
        # Commit as mudanças
        conn.commit()
        
        logger.info(f"Consolidation completed: {consolidated_count} groups consolidated, {deleted_count} duplicates removed")
        print(f"✅ Consolidation completed: {consolidated_count} groups consolidated, {deleted_count} duplicates removed")
        
        # Verificar resultado
        final_duplicates = conn.execute("""
            SELECT cnpj, COUNT(*) as count
            FROM establishments 
            GROUP BY cnpj 
            HAVING COUNT(*) > 1
        """).fetchall()
        
        if final_duplicates:
            logger.warning(f"Still {len(final_duplicates)} CNPJ groups with duplicates after consolidation")
            for dup in final_duplicates:
                logger.warning(f"  CNPJ {dup[0]}: {dup[1]} records")
        else:
            logger.info("No duplicates remaining after consolidation")
            print("✅ No duplicates remaining")
            
    except Exception as e:
        logger.error(f"Error during consolidation: {e}", exc_info=True)
        print(f"❌ Error during consolidation: {e}")
        raise


def choose_best_record(records):
    """Escolher o melhor registro entre duplicatas"""
    # Critérios para escolher o melhor:
    # 1. Registro com establishment_type preenchido
    # 2. Registro com mais campos preenchidos
    # 3. Registro mais recente
    
    def score_record(record):
        score = 0
        
        # Pontos por ter establishment_type
        if record[3] and record[3] != 'Outros':  # establishment_type
            score += 10
        
        # Pontos por campos preenchidos
        fields_to_check = [4, 5, 6, 7]  # address, city, state, cnae_code
        for field_idx in fields_to_check:
            if record[field_idx]:
                score += 1
        
        # Data de criação (mais recente = melhor, mas com peso menor)
        # record[8] é created_at
        
        return score
    
    # Ordenar por score (maior primeiro) e depois por data (mais recente primeiro)
    scored_records = [(score_record(r), r) for r in records]
    scored_records.sort(key=lambda x: (x[0], x[1][8]), reverse=True)
    
    return scored_records[0][1]  # Retorna o melhor registro


if __name__ == "__main__":
    consolidate_establishments()