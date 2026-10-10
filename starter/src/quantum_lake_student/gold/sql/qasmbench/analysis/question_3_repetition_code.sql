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
-- Groups by data qubit to present a concise mapping (3 rows).

SELECT
    dq.data_qubit,
    string_agg(DISTINCT s.ancilla_qubit, ', ' ORDER BY s.ancilla_qubit) AS parity_ancillas,
    string_agg(DISTINCT s.syndrome_bit, ', ' ORDER BY s.syndrome_bit) AS syndrome_bits,
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
GROUP BY 
    dq.data_qubit, 
    cc.condition_register, 
    cc.condition_value, 
    cc.gate
ORDER BY dq.data_qubit;
