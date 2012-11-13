This assignment for the parallel programming course consists of writing a parallel program in Java using the Ibis system and running it on the DAS-4.

The goal of the assignment is to write an application which is able to determine the shortest number of twists possible, as well as the number of ways to solve the cube, given a certain cube. A sequential solver is available in this page. The application includes a build.xml file for building the application with ant, as well as a doc and bin directory to hold any documentation, scripts and external dependencies for your application.

Although the application randomly creates cubes it is deterministic, with a given setting and seed it will always generate and solve the same cube, allowing for easy benchmarking of the application. The different command line parameters drasticly change the application runs. If the default parameters do not work for you, try to adjust them somewhat.
