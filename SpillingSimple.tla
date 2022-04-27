--------------------------- MODULE SpillingSimple ---------------------------
EXTENDS TLC, Naturals, Sequences

CONSTANT MAX_MEMORY, BUILD_BATCHES, PROBE_BATCHES, PARTITIONS, THREADS

(* --algorithm Spilling

variables build_accum = 0;
          probe_accum = 0;
          im_partitions_build = [i \in 1..PARTITIONS |-> 0];
          im_partitions_probe = [i \in 1..PARTITIONS |-> 0];
          od_partitions_build = [i \in 1..PARTITIONS |-> 0];
          od_partitions_probe = [i \in 1..PARTITIONS |-> 0];
          num_build_left = BUILD_BATCHES;
          num_probe_left = PROBE_BATCHES;
          spilling = FALSE;
          memory_used = 0;
          in_flight = 0;
          cursor = 0;
          back_pressure = FALSE;

define 
RECURSIVE Sum(_, _)
Sum(f, n) == IF n = 1 THEN f[n] ELSE f[n] + Sum(f, n - 1)

MemoryInvariant == memory_used <= MAX_MEMORY + PARTITIONS * THREADS
end define;

macro IncPartitions(parts) begin
    parts := [i \in 1..PARTITIONS |-> parts[i] + 1];
end macro;

macro CheckBackPressure() begin
    back_pressure := memory_used > MAX_MEMORY
end macro;

procedure InputReceived_Build() begin
b0: if spilling then
b1:     IncPartitions(im_partitions_build);
b2:     memory_used := memory_used + MAX_MEMORY;
        CheckBackPressure();
b3:     if memory_used > MAX_MEMORY + in_flight /\ cursor < PARTITIONS then
            cursor := cursor + 1;
        end if;
    else
b4:     build_accum := build_accum + PARTITIONS;
        memory_used := memory_used + PARTITIONS;
b5:     if memory_used > MAX_MEMORY then
b6:         spilling := TRUE;
b7:         back_pressure := TRUE;
        end if;
    end if;
b8: return;
end procedure;

procedure InputReceived_Probe()begin
p0: if spilling then
p1:     IncPartitions(im_partitions_probe);
p2:     memory_used := memory_used + MAX_MEMORY;
        CheckBackPressure();
p3:     if memory_used > MAX_MEMORY + in_flight /\ cursor < PARTITIONS then
            cursor := cursor + 1;
        end if;
    else
p4:     probe_accum := probe_accum + PARTITIONS;
        memory_used := memory_used + PARTITIONS;
p5:     if memory_used > MAX_MEMORY then
p6:         spilling := TRUE;
p7:         back_pressure := TRUE;
        end if;
    end if;
p8: return;
end procedure;

process Source \in 1..THREADS begin
s0: while TRUE do
        either
            if num_build_left > 0 /\ back_pressure = FALSE then
                num_build_left := num_build_left - 1;
                call InputReceived_Build();
            end if;
        or
            if num_probe_left > 0 /\ back_pressure = FALSE then
                num_probe_left := num_probe_left - 1;
                call InputReceived_Probe();
            end if;
        or
            if spilling /\ build_accum /= 0 then
                IncPartitions(im_partitions_build);
                build_accum := build_accum - PARTITIONS;
s1:             if build_accum = 0 /\ probe_accum = 0 then
                    cursor := 1;
                end if;
            end if;
        or
            if spilling /\ probe_accum /= 0 then
                IncPartitions(im_partitions_probe);
                probe_accum := probe_accum - PARTITIONS;
s2:             if build_accum = 0 /\ probe_accum = 0 then
                    cursor := 1;
                end if;
            end if;
        or
            if num_probe_left = 0 /\ num_build_left = 0 /\ probe_accum = 0 /\ build_accum = 0 then
                goto s_done;
            end if;
        end either;       
    end while;
s_done:
    skip;
end process;

process Disk = (THREADS + 1)
begin
d0: await cursor /= 0;
d1: while TRUE do
        in_flight := in_flight + Sum(im_partitions_build, cursor) + Sum(im_partitions_probe, cursor);
        either
d2:         with part \in {i \in 1..cursor : im_partitions_build[i] /= 0} do
                in_flight := in_flight - im_partitions_build[part];
                od_partitions_build[part] := od_partitions_build[part] + im_partitions_build[part];
                memory_used := memory_used - im_partitions_build[part];
                im_partitions_build[part] := 0;
                CheckBackPressure();
            end with;    
        or
d3:         with part \in {i \in 1..cursor : im_partitions_probe[i] /= 0} do
                in_flight := in_flight - im_partitions_probe[part];
                od_partitions_probe[part] := od_partitions_probe[part] + im_partitions_probe[part];
                memory_used := memory_used - im_partitions_probe[part];
                im_partitions_probe[part] := 0;
                CheckBackPressure();
            end with;
        end either;
    end while;
end process;

end algorithm *)
\* BEGIN TRANSLATION (chksum(pcal) = "4f36b90f" /\ chksum(tla) = "fa3b28f8")
VARIABLES build_accum, probe_accum, im_partitions_build, im_partitions_probe, 
          od_partitions_build, od_partitions_probe, num_build_left, 
          num_probe_left, spilling, memory_used, in_flight, cursor, 
          back_pressure, pc, stack

(* define statement *)
RECURSIVE Sum(_, _)
Sum(f, n) == IF n = 1 THEN f[n] ELSE f[n] + Sum(f, n - 1)

MemoryInvariant == memory_used <= MAX_MEMORY + PARTITIONS * THREADS


vars == << build_accum, probe_accum, im_partitions_build, im_partitions_probe, 
           od_partitions_build, od_partitions_probe, num_build_left, 
           num_probe_left, spilling, memory_used, in_flight, cursor, 
           back_pressure, pc, stack >>

ProcSet == (1..THREADS) \cup {(THREADS + 1)}

Init == (* Global variables *)
        /\ build_accum = 0
        /\ probe_accum = 0
        /\ im_partitions_build = [i \in 1..PARTITIONS |-> 0]
        /\ im_partitions_probe = [i \in 1..PARTITIONS |-> 0]
        /\ od_partitions_build = [i \in 1..PARTITIONS |-> 0]
        /\ od_partitions_probe = [i \in 1..PARTITIONS |-> 0]
        /\ num_build_left = BUILD_BATCHES
        /\ num_probe_left = PROBE_BATCHES
        /\ spilling = FALSE
        /\ memory_used = 0
        /\ in_flight = 0
        /\ cursor = 0
        /\ back_pressure = FALSE
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in 1..THREADS -> "s0"
                                        [] self = (THREADS + 1) -> "d0"]

b0(self) == /\ pc[self] = "b0"
            /\ IF spilling
                  THEN /\ pc' = [pc EXCEPT ![self] = "b1"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "b4"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, back_pressure, stack >>

b1(self) == /\ pc[self] = "b1"
            /\ im_partitions_build' = [i \in 1..PARTITIONS |-> im_partitions_build[i] + 1]
            /\ pc' = [pc EXCEPT ![self] = "b2"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_probe, 
                            od_partitions_build, od_partitions_probe, 
                            num_build_left, num_probe_left, spilling, 
                            memory_used, in_flight, cursor, back_pressure, 
                            stack >>

b2(self) == /\ pc[self] = "b2"
            /\ memory_used' = memory_used + MAX_MEMORY
            /\ back_pressure' = (memory_used' > MAX_MEMORY)
            /\ pc' = [pc EXCEPT ![self] = "b3"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, in_flight, cursor, stack >>

b3(self) == /\ pc[self] = "b3"
            /\ IF memory_used > MAX_MEMORY + in_flight /\ cursor < PARTITIONS
                  THEN /\ cursor' = cursor + 1
                  ELSE /\ TRUE
                       /\ UNCHANGED cursor
            /\ pc' = [pc EXCEPT ![self] = "b8"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            back_pressure, stack >>

b4(self) == /\ pc[self] = "b4"
            /\ build_accum' = build_accum + PARTITIONS
            /\ memory_used' = memory_used + PARTITIONS
            /\ pc' = [pc EXCEPT ![self] = "b5"]
            /\ UNCHANGED << probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, in_flight, cursor, 
                            back_pressure, stack >>

b5(self) == /\ pc[self] = "b5"
            /\ IF memory_used > MAX_MEMORY
                  THEN /\ pc' = [pc EXCEPT ![self] = "b6"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "b8"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, back_pressure, stack >>

b6(self) == /\ pc[self] = "b6"
            /\ spilling' = TRUE
            /\ pc' = [pc EXCEPT ![self] = "b7"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, memory_used, in_flight, cursor, 
                            back_pressure, stack >>

b7(self) == /\ pc[self] = "b7"
            /\ back_pressure' = TRUE
            /\ pc' = [pc EXCEPT ![self] = "b8"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, stack >>

b8(self) == /\ pc[self] = "b8"
            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, back_pressure >>

InputReceived_Build(self) == b0(self) \/ b1(self) \/ b2(self) \/ b3(self)
                                \/ b4(self) \/ b5(self) \/ b6(self)
                                \/ b7(self) \/ b8(self)

p0(self) == /\ pc[self] = "p0"
            /\ IF spilling
                  THEN /\ pc' = [pc EXCEPT ![self] = "p1"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "p4"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, back_pressure, stack >>

p1(self) == /\ pc[self] = "p1"
            /\ im_partitions_probe' = [i \in 1..PARTITIONS |-> im_partitions_probe[i] + 1]
            /\ pc' = [pc EXCEPT ![self] = "p2"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            od_partitions_build, od_partitions_probe, 
                            num_build_left, num_probe_left, spilling, 
                            memory_used, in_flight, cursor, back_pressure, 
                            stack >>

p2(self) == /\ pc[self] = "p2"
            /\ memory_used' = memory_used + MAX_MEMORY
            /\ back_pressure' = (memory_used' > MAX_MEMORY)
            /\ pc' = [pc EXCEPT ![self] = "p3"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, in_flight, cursor, stack >>

p3(self) == /\ pc[self] = "p3"
            /\ IF memory_used > MAX_MEMORY + in_flight /\ cursor < PARTITIONS
                  THEN /\ cursor' = cursor + 1
                  ELSE /\ TRUE
                       /\ UNCHANGED cursor
            /\ pc' = [pc EXCEPT ![self] = "p8"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            back_pressure, stack >>

p4(self) == /\ pc[self] = "p4"
            /\ probe_accum' = probe_accum + PARTITIONS
            /\ memory_used' = memory_used + PARTITIONS
            /\ pc' = [pc EXCEPT ![self] = "p5"]
            /\ UNCHANGED << build_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, in_flight, cursor, 
                            back_pressure, stack >>

p5(self) == /\ pc[self] = "p5"
            /\ IF memory_used > MAX_MEMORY
                  THEN /\ pc' = [pc EXCEPT ![self] = "p6"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "p8"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, back_pressure, stack >>

p6(self) == /\ pc[self] = "p6"
            /\ spilling' = TRUE
            /\ pc' = [pc EXCEPT ![self] = "p7"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, memory_used, in_flight, cursor, 
                            back_pressure, stack >>

p7(self) == /\ pc[self] = "p7"
            /\ back_pressure' = TRUE
            /\ pc' = [pc EXCEPT ![self] = "p8"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, stack >>

p8(self) == /\ pc[self] = "p8"
            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            cursor, back_pressure >>

InputReceived_Probe(self) == p0(self) \/ p1(self) \/ p2(self) \/ p3(self)
                                \/ p4(self) \/ p5(self) \/ p6(self)
                                \/ p7(self) \/ p8(self)

s0(self) == /\ pc[self] = "s0"
            /\ \/ /\ IF num_build_left > 0 /\ back_pressure = FALSE
                        THEN /\ num_build_left' = num_build_left - 1
                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "InputReceived_Build",
                                                                      pc        |->  "s0" ] >>
                                                                  \o stack[self]]
                             /\ pc' = [pc EXCEPT ![self] = "b0"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "s0"]
                             /\ UNCHANGED << num_build_left, stack >>
                  /\ UNCHANGED <<build_accum, probe_accum, im_partitions_build, im_partitions_probe, num_probe_left>>
               \/ /\ IF num_probe_left > 0 /\ back_pressure = FALSE
                        THEN /\ num_probe_left' = num_probe_left - 1
                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "InputReceived_Probe",
                                                                      pc        |->  "s0" ] >>
                                                                  \o stack[self]]
                             /\ pc' = [pc EXCEPT ![self] = "p0"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "s0"]
                             /\ UNCHANGED << num_probe_left, stack >>
                  /\ UNCHANGED <<build_accum, probe_accum, im_partitions_build, im_partitions_probe, num_build_left>>
               \/ /\ IF spilling /\ build_accum /= 0
                        THEN /\ im_partitions_build' = [i \in 1..PARTITIONS |-> im_partitions_build[i] + 1]
                             /\ build_accum' = build_accum - PARTITIONS
                             /\ pc' = [pc EXCEPT ![self] = "s1"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "s0"]
                             /\ UNCHANGED << build_accum, im_partitions_build >>
                  /\ UNCHANGED <<probe_accum, im_partitions_probe, num_build_left, num_probe_left, stack>>
               \/ /\ IF spilling /\ probe_accum /= 0
                        THEN /\ im_partitions_probe' = [i \in 1..PARTITIONS |-> im_partitions_probe[i] + 1]
                             /\ probe_accum' = probe_accum - PARTITIONS
                             /\ pc' = [pc EXCEPT ![self] = "s2"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "s0"]
                             /\ UNCHANGED << probe_accum, im_partitions_probe >>
                  /\ UNCHANGED <<build_accum, im_partitions_build, num_build_left, num_probe_left, stack>>
               \/ /\ IF num_probe_left = 0 /\ num_build_left = 0 /\ probe_accum = 0 /\ build_accum = 0
                        THEN /\ pc' = [pc EXCEPT ![self] = "s_done"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "s0"]
                  /\ UNCHANGED <<build_accum, probe_accum, im_partitions_build, im_partitions_probe, num_build_left, num_probe_left, stack>>
            /\ UNCHANGED << od_partitions_build, od_partitions_probe, spilling, 
                            memory_used, in_flight, cursor, back_pressure >>

s1(self) == /\ pc[self] = "s1"
            /\ IF build_accum = 0 /\ probe_accum = 0
                  THEN /\ cursor' = 1
                  ELSE /\ TRUE
                       /\ UNCHANGED cursor
            /\ pc' = [pc EXCEPT ![self] = "s0"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            back_pressure, stack >>

s2(self) == /\ pc[self] = "s2"
            /\ IF build_accum = 0 /\ probe_accum = 0
                  THEN /\ cursor' = 1
                  ELSE /\ TRUE
                       /\ UNCHANGED cursor
            /\ pc' = [pc EXCEPT ![self] = "s0"]
            /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                            im_partitions_probe, od_partitions_build, 
                            od_partitions_probe, num_build_left, 
                            num_probe_left, spilling, memory_used, in_flight, 
                            back_pressure, stack >>

s_done(self) == /\ pc[self] = "s_done"
                /\ TRUE
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                                im_partitions_probe, od_partitions_build, 
                                od_partitions_probe, num_build_left, 
                                num_probe_left, spilling, memory_used, 
                                in_flight, cursor, back_pressure, stack >>

Source(self) == s0(self) \/ s1(self) \/ s2(self) \/ s_done(self)

d0 == /\ pc[(THREADS + 1)] = "d0"
      /\ cursor /= 0
      /\ pc' = [pc EXCEPT ![(THREADS + 1)] = "d1"]
      /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                      im_partitions_probe, od_partitions_build, 
                      od_partitions_probe, num_build_left, num_probe_left, 
                      spilling, memory_used, in_flight, cursor, back_pressure, 
                      stack >>

d1 == /\ pc[(THREADS + 1)] = "d1"
      /\ in_flight' = in_flight + Sum(im_partitions_build, cursor) + Sum(im_partitions_probe, cursor)
      /\ \/ /\ pc' = [pc EXCEPT ![(THREADS + 1)] = "d2"]
         \/ /\ pc' = [pc EXCEPT ![(THREADS + 1)] = "d3"]
      /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                      im_partitions_probe, od_partitions_build, 
                      od_partitions_probe, num_build_left, num_probe_left, 
                      spilling, memory_used, cursor, back_pressure, stack >>

d2 == /\ pc[(THREADS + 1)] = "d2"
      /\ \E part \in {i \in 1..cursor : im_partitions_build[i] /= 0}:
           /\ in_flight' = in_flight - im_partitions_build[part]
           /\ od_partitions_build' = [od_partitions_build EXCEPT ![part] = od_partitions_build[part] + im_partitions_build[part]]
           /\ memory_used' = memory_used - im_partitions_build[part]
           /\ im_partitions_build' = [im_partitions_build EXCEPT ![part] = 0]
           /\ back_pressure' = (memory_used' > MAX_MEMORY)
      /\ pc' = [pc EXCEPT ![(THREADS + 1)] = "d1"]
      /\ UNCHANGED << build_accum, probe_accum, im_partitions_probe, 
                      od_partitions_probe, num_build_left, num_probe_left, 
                      spilling, cursor, stack >>

d3 == /\ pc[(THREADS + 1)] = "d3"
      /\ \E part \in {i \in 1..cursor : im_partitions_probe[i] /= 0}:
           /\ in_flight' = in_flight - im_partitions_probe[part]
           /\ od_partitions_probe' = [od_partitions_probe EXCEPT ![part] = od_partitions_probe[part] + im_partitions_probe[part]]
           /\ memory_used' = memory_used - im_partitions_probe[part]
           /\ im_partitions_probe' = [im_partitions_probe EXCEPT ![part] = 0]
           /\ back_pressure' = (memory_used' > MAX_MEMORY)
      /\ pc' = [pc EXCEPT ![(THREADS + 1)] = "d1"]
      /\ UNCHANGED << build_accum, probe_accum, im_partitions_build, 
                      od_partitions_build, num_build_left, num_probe_left, 
                      spilling, cursor, stack >>

Disk == d0 \/ d1 \/ d2 \/ d3

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Disk
           \/ (\E self \in ProcSet:  \/ InputReceived_Build(self)
                                     \/ InputReceived_Probe(self))
           \/ (\E self \in 1..THREADS: Source(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
\* Modification History
\* Last modified Wed Apr 27 13:49:41 PDT 2022 by sasha
\* Created Tue Apr 26 11:03:09 PDT 2022 by sasha
