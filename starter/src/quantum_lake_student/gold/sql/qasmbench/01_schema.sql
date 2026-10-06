-- Gold relational model for the QASMBench source (PostgreSQL 16).
--
-- Architecture & Medallion Layering:
-- Normalizes quantum circuit metadata, register declarations, stabilizer
-- parity checks, data-qubit mappings, and conditional feedback operations.
--
-- Re-runnable: the objects owned by this file are dropped and rebuilt in ONE
-- transaction with the data load. If anything fails, PostgreSQL rolls it all back
-- and the previous complete version remains available (all-or-nothing load).

CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.conditional_correction CASCADE;
DROP TABLE IF EXISTS gold.stabilizer_data_qubit CASCADE;
DROP TABLE IF EXISTS gold.stabilizer_check CASCADE;
DROP TABLE IF EXISTS gold.circuit_register CASCADE;
DROP TABLE IF EXISTS gold.circuit CASCADE;

-- 1. Circuit Variant Table
-- One row = one quantum benchmark circuit variant (source or transpiled).
CREATE TABLE IF NOT EXISTS gold.circuit (
    circuit_id            VARCHAR(64) PRIMARY KEY,
    benchmark_name        VARCHAR(64) NOT NULL,
    variant               VARCHAR(32) NOT NULL CHECK (variant IN ('source', 'transpiled')),
    qubit_count           INTEGER NOT NULL CHECK (qubit_count > 0),
    measurement_count     INTEGER NOT NULL CHECK (measurement_count >= 0),
    two_qubit_gate_count  INTEGER NOT NULL CHECK (two_qubit_gate_count >= 0),
    source_record_id      VARCHAR(255) NOT NULL UNIQUE,
    UNIQUE (benchmark_name, qubit_count, variant)
);

COMMENT ON TABLE gold.circuit IS
    'One row = one quantum benchmark circuit variant with structural gate and qubit metrics.';
COMMENT ON COLUMN gold.circuit.circuit_id IS 'Unique identifier of circuit variant (e.g., qec_sm_n5).';
COMMENT ON COLUMN gold.circuit.variant IS 'Circuit variant type: source or transpiled.';
COMMENT ON COLUMN gold.circuit.source_record_id IS 'Silver lineage pointer.';

-- 2. Circuit Register Table
-- One row = one quantum or classical register declared in a circuit.
CREATE TABLE IF NOT EXISTS gold.circuit_register (
    circuit_id     VARCHAR(64) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE,
    register_name  VARCHAR(64) NOT NULL,
    register_type  VARCHAR(16) NOT NULL CHECK (register_type IN ('qreg', 'creg')),
    size           INTEGER NOT NULL CHECK (size > 0),
    PRIMARY KEY (circuit_id, register_name)
);

COMMENT ON TABLE gold.circuit_register IS
    'One row = one quantum (qreg) or classical (creg) register allocated in a circuit.';

-- 3. Stabilizer Parity Check Table
-- One row = one syndrome stabilizer measurement check in a QEC circuit.
CREATE TABLE IF NOT EXISTS gold.stabilizer_check (
    circuit_id        VARCHAR(64) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE,
    check_id          VARCHAR(128) NOT NULL,
    ancilla_qubit     VARCHAR(32) NOT NULL,
    syndrome_bit      VARCHAR(32) NOT NULL,
    source_record_id  VARCHAR(255) NOT NULL,
    PRIMARY KEY (circuit_id, check_id)
);

COMMENT ON TABLE gold.stabilizer_check IS
    'One row = one stabilizer parity check mapping an ancilla qubit to a classical syndrome bit.';

-- 4. Stabilizer Data Qubit Association Table
-- One row = one data qubit participating in a stabilizer parity check.
CREATE TABLE IF NOT EXISTS gold.stabilizer_data_qubit (
    circuit_id   VARCHAR(64) NOT NULL,
    check_id     VARCHAR(128) NOT NULL,
    data_qubit   VARCHAR(32) NOT NULL,
    PRIMARY KEY (circuit_id, check_id, data_qubit),
    FOREIGN KEY (circuit_id, check_id) REFERENCES gold.stabilizer_check(circuit_id, check_id) ON DELETE CASCADE,
    FOREIGN KEY (circuit_id) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE
);

COMMENT ON TABLE gold.stabilizer_data_qubit IS
    'Association table resolving the many-to-many relationship between data qubits and stabilizer checks.';

-- 5. Conditional Syndrome Correction Table
-- One row = one feedforward recovery correction triggered by classical syndrome values.
CREATE TABLE IF NOT EXISTS gold.conditional_correction (
    circuit_id          VARCHAR(64) REFERENCES gold.circuit(circuit_id) ON DELETE CASCADE,
    condition_register  VARCHAR(32) NOT NULL,
    condition_value     INTEGER NOT NULL CHECK (condition_value >= 0),
    target_qubit        VARCHAR(32) NOT NULL,
    gate                VARCHAR(16) NOT NULL,
    source_record_id    VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (circuit_id, target_qubit, condition_register, condition_value)
);

COMMENT ON TABLE gold.conditional_correction IS
    'One row = one conditional feedforward recovery gate conditioned on a syndrome register value.';
