## Blue Jays Data Engineer Technical Exercise

### Part 1: Table Definitions
The table definitions for **Part 1** are located in the  
`part_1_table_ddls` directory.

### Part 2: Data Loading
The code for **Part 2** is located in the  
`part_2_load_tables` directory.

To run the code:
1. Open `config.yaml`
2. Update the `database_uri` value to point to the database you want to use

For the runner_play table, is_firsttothird and is_secondtohome were designated as True regardless of whether the runner was safe or out.
reachedbase was assigned NULL if there was no data record of the runner advancing to a base safely on the play.

### Part 3: Queries
The queries for **Part 3** are located in the  
`part_3_queries` directory.

---

## Query Results

### 3a) Top 5 Regular Season Wins
1. **Milwaukee Brewers** — 97 wins
2. **Philadelphia Phillies** — 96 wins
3. **Toronto Blue Jays** — 94 wins (tie)
3. **New York Yankees** — 94 wins (tie)
5. **Los Angeles Dodgers** — 93 wins

---

### 3b) Players With at Least 35 Regular Season Stolen Bases
- **José Caballero** — 49 stolen bases
- **Chandler Simpson** — 44 stolen bases
- **José Ramírez** — 44 stolen bases
- **Bobby Witt Jr.** — 39 stolen bases
- **Juan Soto** — 38 stolen bases
- **Oneil Cruz** — 38 stolen bases
- **Elly De La Cruz** — 37 stolen bases
- **Trea Turner** — 36 stolen bases
- **Pete Crow-Armstrong** — 35 stolen bases

---

> **Note:** For Parts **3c–3f**, *all game types* were counted.

---

### 3c) Most Extra Bases Taken on Singles and Doubles
1. **Elly De La Cruz** — 47 extra bases taken
2. **George Springer** — 43 extra bases taken
3. **Ernie Clement** — 42 extra bases taken
4. **Maikel Garcia** — 41 extra bases taken
5. **Mookie Betts** — 40 extra bases taken
6. **Aaron Judge** — 38 extra bases taken
7. **TJ Friedl** — 37 (tie) extra bases taken
8. **Fernando Tatis Jr.** — 37 (tie) extra bases taken
9. **Brice Turang** — 36 (tie)  extra bases taken
10. **CJ Abrams** — 36 (tie)  extra bases taken

---

### 3d) Largest Deficit Comeback
The **Colorado Rockies** came back from a **9-run deficit** against the  
**Pittsburgh Pirates** in **gamepk 776919**.

---

### 3e) Most Come-From-Behind Wins
1. **Toronto Blue Jays** — 63 comebacks
2. **Los Angeles Dodgers** — 61 comebacks
3. **Seattle Mariners** — 55 comebacks
4. **Philadelphia Phillies** — 54 comebacks
5. **Kansas City Royals** — 49 comebacks

---

### 3f) Most Aggressive Baserunners
To identify the most aggressive baserunners, all aggressive actions taken by a runner were counted. These include:
- Extra bases attempted on singles and doubles (including attempts being thrown out)
- Attempted stolen bases
- Times picked off (aggressive leadoffs)

**Top 5:**
1. **Elly De La Cruz** — 100 aggressive actions
2. **Maikel Garcia** — 91 aggressive actions
3. **Chandler Simpson** — 91 aggressive actions
4. **José Ramírez** — 89 aggressive actions
5. **José Caballero** — 86 aggressive actions  
