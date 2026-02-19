from src.analytics.gold_layer import GoldLayerETL

def main():
    etl = GoldLayerETL()
    
    # Usar una partición que YA EXISTE en Silver (de Fase 3)
    raid_id = "raid001"
    event_date = "2026-02-19"  # ajusta a tu fecha real
    
    result = etl.run_for_partition(raid_id, event_date)
    
    print("\n=== Resultado ===")
    print(result)

if __name__ == "__main__":
    main()