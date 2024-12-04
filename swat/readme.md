For **Python 3.12 on Windows only**, the following modification was made to the `pyport.h` file while building the SWAT C extensions:  

* Updated the `#define` for `ALWAYS_INLINE`
  <br>**Previous Definition :**
  ```c 
  #elif defined(__GNUC__) || defined(__clang__) || defined(__INTEL_COMPILER)
  ```
  **Updated Definition :**
  ```c
  #elif defined(__GNUC__) || defined(__clang__) || defined(__INTEL_LLVM_COMPILER) || (defined(__INTEL_COMPILER) && !defined(_WIN32))
  ```

This change addresses a compiler error encountered when using the Intel compiler on Windows.
