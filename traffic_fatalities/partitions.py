from dagster import DynamicPartitionsDefinition, StaticPartitionsDefinition

nodes_partitions_def = DynamicPartitionsDefinition(name="intersections")
consolidation_tolerances_partitions_def = StaticPartitionsDefinition(['1', '5', '10', '15', '20', '25', '30'])