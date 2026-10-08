-- Part I Analysis — Question 3: Repetition Code Circuit Mapping.
--
-- Question: "How does the repetition-code circuit map data qubits to parity-check
-- ancillas, syndrome bits, and conditional corrections?"
--
-- Joins 4 PostgreSQL Gold tables:
--   1. gold.circuit (c)
--   2. gold.stabilizer_check (s)
--   3. gold.stabilizer_data_qubit (dq)
--   4. gold.conditional_correction (cc)
--
-- Evaluated specifically on the small 5-qubit repetition code ('qec_sm_n5').
-- Returns 1NF relational mapping with 4 rows.

SELECT
    dq.data_qubit,
    s.ancilla_qubit,
    s.syndrome_bit,
    cc.condition_register,
    cc.condition_value,
    cc.gate AS recovery_gate
FROM circuit c
JOIN stabilizer_check s
    ON c.circuit_id = s.circuit_id
JOIN stabilizer_data_qubit dq
    ON s.circuit_id = dq.circuit_id
   AND s.check_id = dq.check_id
JOIN conditional_correction cc
    ON c.circuit_id = cc.circuit_id
   AND dq.data_qubit = cc.target_qubit
WHERE c.circuit_id = 'qec_sm_n5'
ORDER BY dq.data_qubit, s.check_id;
