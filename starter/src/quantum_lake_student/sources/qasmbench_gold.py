"""QASMBench Gold relational schema and PostgreSQL loader.

This module defines the relational DDL for QASMBench in PostgreSQL (under schema 'gold')
and provides atomic, idempotent loading from Silver Parquet tables into Gold relations.
"""

from __future__ import annotations

import json
from typing import Any
import psycopg

from quantum_lake_student.config import Settings
from quantum_lake_student.connections import postgres_connection
from quantum_lake_student.sources.qasmbench_silver import read_lake_table


DDL_STATEMENTS = [
    # 1. Gold Schema
    "CREATE SCHEMA IF NOT EXISTS gold;",

    # 2. Circuit Variant Table
    """
    CREATE TABLE IF NOT EXISTS gold.circuit (
        circuit_id VARCHAR(64) PRIMARY KEY,
        benchmark_name VARCHAR(64) NOT NULL,
        variant VARCHAR(32) NOT NULL CHECK (variant IN ('source', 'transpiled')),
        qubit_count INTEGER NOT NULL CHECK (qubit_count > 0),
        measurement_count INTEGER NOT NULL CHECK (measurement_count >= 0),
        two_qubit_gate_count INTEGER NOT NULL CHECK (two_qubit_gate_count >= 0),
        source_record_id VARCHAR(255) NOT NULL
    );
    """,

    # 3. Circuit Register Table 
    """
    CREATE TABLE IF NOT EXISTS gold.circuit_register (
        circuit_id VARCHAR(64) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE,
        register_name VARCHAR(64) NOT NULL,
        register_type VARCHAR(16) NOT NULL CHECK (register_type IN ('qreg', 'creg')),
        size INTEGER NOT NULL CHECK (size > 0),
        PRIMARY KEY (circuit_id, register_name)
    );
    """,

    # 4. Stabilizer Parity Check Table
    """
    CREATE TABLE IF NOT EXISTS gold.stabilizer_check (
        circuit_id VARCHAR(64) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE,
        check_id VARCHAR(128) NOT NULL,
        ancilla_qubit VARCHAR(32) NOT NULL,
        syndrome_bit VARCHAR(32) NOT NULL,
        source_record_id VARCHAR(255) NOT NULL,
        PRIMARY KEY (circuit_id, check_id)
    );
    """,

    # 5. Stabilizer Data Qubit Association Table 
    """
    CREATE TABLE IF NOT EXISTS gold.stabilizer_data_qubit (
        circuit_id VARCHAR(64) NOT NULL,
        check_id VARCHAR(128) NOT NULL,
        data_qubit VARCHAR(32) NOT NULL,
        PRIMARY KEY (circuit_id, check_id, data_qubit),
        FOREIGN KEY (circuit_id, check_id) REFERENCES gold.stabilizer_check(circuit_id, check_id) ON DELETE CASCADE,
        FOREIGN KEY (circuit_id) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE
    );
    """,

    # 6. Conditional Syndrome Correction Table (Natural Domain Key)
    """
    CREATE TABLE IF NOT EXISTS gold.conditional_correction (
        circuit_id VARCHAR(64) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE,
        condition_register VARCHAR(32) NOT NULL,
        condition_value INTEGER NOT NULL CHECK (condition_value >= 0),
        target_qubit VARCHAR(32) NOT NULL,
        gate VARCHAR(16) NOT NULL,
        source_record_id VARCHAR(255) NOT NULL,
        PRIMARY KEY (circuit_id, target_qubit, condition_register, condition_value)
    );
    """,
]


def init_schema(conn: psycopg.Connection) -> None:
    """Create the gold schema, tables, constraints, and indexes if they do not exist."""
    with conn.cursor() as cur:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)


def load_qasmbench_gold(
    conn: psycopg.Connection,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Read QASMBench Silver Parquet tables from MinIO and load them atomically into PostgreSQL Gold.

    Uses an idempotent transaction so repeated executions update existing records
    without creating duplicates.
    """
    if settings is None:
        settings = Settings.from_environment()

    # 1. Ensure DDL exists
    init_schema(conn)

    # 2. Read Silver Parquet tables
    circuit_table = read_lake_table("silver/qasmbench/circuit.parquet", settings)
    stab_table = read_lake_table("silver/qasmbench/stabilizer_check.parquet", settings)
    cond_table = read_lake_table("silver/qasmbench/conditional_correction.parquet", settings)

    circuit_rows = circuit_table.to_pylist()
    stab_rows = stab_table.to_pylist()
    cond_rows = cond_table.to_pylist()

    counts = {
        "circuit": 0,
        "circuit_register": 0,
        "stabilizer_check": 0,
        "stabilizer_data_qubit": 0,
        "conditional_correction": 0,
    }

    # 3. Load within an atomic transaction
    with conn.transaction():
        with conn.cursor() as cur:
            # 3a. Populate gold.circuit & gold.circuit_register
            for row in circuit_rows:
                reg_decl = row["register_declarations"]
                if isinstance(reg_decl, str):
                    reg_decl_dict = json.loads(reg_decl)
                else:
                    reg_decl_dict = reg_decl

                cur.execute(
                    """
                    INSERT INTO gold.circuit (
                        circuit_id, benchmark_name, variant, qubit_count,
                        measurement_count, two_qubit_gate_count, source_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id) DO UPDATE SET
                        benchmark_name = EXCLUDED.benchmark_name,
                        variant = EXCLUDED.variant,
                        qubit_count = EXCLUDED.qubit_count,
                        measurement_count = EXCLUDED.measurement_count,
                        two_qubit_gate_count = EXCLUDED.two_qubit_gate_count,
                        source_record_id = EXCLUDED.source_record_id;
                    """,
                    (
                        row["circuit_id"],
                        row["benchmark_name"],
                        row["variant"],
                        row["qubit_count"],
                        row["measurement_count"],
                        row["two_qubit_gate_count"],
                        row["source_record_id"],
                    ),
                )
                counts["circuit"] += 1

                # Normalize registers into gold.circuit_register
                for reg_type, regs in [("qreg", reg_decl_dict.get("qregs", {})), ("creg", reg_decl_dict.get("cregs", {}))]:
                    for reg_name, size in regs.items():
                        cur.execute(
                            """
                            INSERT INTO gold.circuit_register (
                                circuit_id, register_name, register_type, size
                            ) VALUES (%s, %s, %s, %s)
                            ON CONFLICT (circuit_id, register_name) DO UPDATE SET
                                register_type = EXCLUDED.register_type,
                                size = EXCLUDED.size;
                            """,
                            (row["circuit_id"], reg_name, reg_type, int(size)),
                        )
                        counts["circuit_register"] += 1

            # 3b. Populate gold.stabilizer_check & gold.stabilizer_data_qubit
            for row in stab_rows:
                cur.execute(
                    """
                    INSERT INTO gold.stabilizer_check (
                        circuit_id, check_id, ancilla_qubit, syndrome_bit, source_record_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id, check_id) DO UPDATE SET
                        ancilla_qubit = EXCLUDED.ancilla_qubit,
                        syndrome_bit = EXCLUDED.syndrome_bit,
                        source_record_id = EXCLUDED.source_record_id;
                    """,
                    (
                        row["circuit_id"],
                        row["check_id"],
                        row["ancilla_qubit"],
                        row["syndrome_bit"],
                        row["source_record_id"],
                    ),
                )
                counts["stabilizer_check"] += 1

                # Normalize data qubits into gold.stabilizer_data_qubit
                d_qubits = row.get("data_qubits", [])
                for dq in d_qubits:
                    cur.execute(
                        """
                        INSERT INTO gold.stabilizer_data_qubit (
                            circuit_id, check_id, data_qubit
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (circuit_id, check_id, data_qubit) DO NOTHING;
                        """,
                        (row["circuit_id"], row["check_id"], dq),
                    )
                    counts["stabilizer_data_qubit"] += 1

            # 3c. Populate gold.conditional_correction using natural domain key
            for row in cond_rows:
                cur.execute(
                    """
                    INSERT INTO gold.conditional_correction (
                        circuit_id, condition_register, condition_value,
                        target_qubit, gate, source_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id, target_qubit, condition_register, condition_value) DO UPDATE SET
                        gate = EXCLUDED.gate,
                        source_record_id = EXCLUDED.source_record_id;
                    """,
                    (
                        row["circuit_id"],
                        row["condition_register"],
                        row["condition_value"],
                        row["target_qubit"],
                        row["gate"],
                        row["source_record_id"],
                    ),
                )
                counts["conditional_correction"] += 1

    return counts


def run_gold_load(settings: Settings | None = None) -> dict[str, int]:
    """Convenience entry point to open connection and execute Gold load."""
    if settings is None:
        settings = Settings.from_environment()

    with postgres_connection(settings) as conn:
        return load_qasmbench_gold(conn, settings)
