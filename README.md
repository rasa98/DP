# Hadoop MapReduce Tasks

### Prerequisites

**Java Version**
* Compiled with Java 20.
* **Requirement:** Java 20 or higher (Tested on Java 21).
* **Note:** Older versions will result in UnsupportedClassVersionError.

**Hadoop Environment**
* Apache Hadoop 3.4.0.
* Execution mode: Standalone.

---

### Project Tasks

1. Word Co-occurrence Network: Forms a directed weighted graph. Nodes represent words; a link A -> B exists if B follows A in any line. Weights represent the probability P(B|A).
2. Positional Inverted Index: Maps tokens to their specific file names and positions (token index).
3. Tweet Similarity: Calculates the frequency distribution of Damerau-Levenshtein distances between a target tweet T (passed as a parameter) and the input dataset.

---

### Setup and Binaries

1. Download the compiled JAR artifacts (`Zad1.jar`, `Zad2.jar`, `Zad3.jar`) from the **Releases** section of this repository.
2. Create a `bin/` directory in the project root if it does not exist.
3. Move the downloaded JAR files into the `bin/` directory.

---

### Execution

The script `scripts/run_tasks.sh` handles output directory cleanup and execution for each task.

| Task | JAR | Main Class |
| :--- | :--- | :--- |
| 1 | Zad1.jar | AfterLine |
| 2 | Zad2.jar | InvertedIndexPos |
| 3 | Zad3.jar | SimilarTweets |

**Commands:**
```bash
cd scripts
chmod +x run_tasks.sh
./run_tasks.sh 1
./run_tasks.sh 2
./run_tasks.sh 3 "give your twwet here"