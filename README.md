# concurrency

## Introduction

This is a repository containing simulations of database concurrency control written in Python. The simulations are not perfect, but they are good enough to demonstrate the basic concepts of concurrency control.

## Usage

Run the desired simulation with the following command:

    python3 <simulation_name>.py

After that, give an input string delimited by ; like follows:

    R5(A);R2(B);R1(B);W3(B);W3(C);R5(C);R2(C);R1(A);R4(D);W3(D);W5(B);W5(C)

Please note that there is no semicolons on the end of the input string.

Feel free to modify the code to suit your needs.
