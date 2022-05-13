# Multi-threaded-Programming
Program that Implements the Map-Reduce algorithm of Multi-threaded-Programming paradigm, using main consepets of Operating Systems (i.e. threads, jobs, mutexes, atpmics, semaphores etc).

High Level Overview:
Performance is the major motivation for multi-threaded programming.
Multiple processors can execute multiple threads at the same time and do the same amount of computations in less time than it will take a single processor.
Two challenges complicate multi-threaded programming:
1) In many cases it is difficult to split the big task into small parts that can run in parallel.
2) Running in multiple threads requires synchronisation and communication between threads. This introduces an overhead which without careful design can increase the total runtime significantly. Over the years, several designs were proposed in order to solve these challenges. 

In this program Iv'e implemented one of these designs, named MapReduce algorithm. MapReduce is used to parallelise tasks of a specific structure. Such tasks are defined by two functions, map and reduce, used as follows:
1) The input is given as a sequence of elements. 
2) (Map phase) The map function is applied to each input element, producing a sequence of intermediary elements.
3) (Sort/Shuffle phases) The intermediary elements are sorted into new sequences (more on this later). 
4) (Reduce phase) The reduce function is applied to each of the sorted sequences of intermediary elements, producing a sequence of output elements. 
5) The output is a concatenation of all sequences of output elements.

Design:  I splited the implementation of this design into two parts:
1) Implementing the functions map and reduce. The functions implementation will be different for every task. This part called the "client".
2) Implementing everything else – the partition into phases, distribution of work between threads, synchronisation etc. This will be identical for different tasks. This part will be the "framework". Using this split, we can code the framework once and then for every new task, we can just code the significantly smaller and simpler client. In other words, after I have written the code for the framework you can write different map and reduce function depending on the task you would like to perform. Constructing the framework was my main goal of this project and that what Iv'e implemented - the framework.

The following diagram contains a summary of the functions in the client and the framework.
An arrow from function a to function b means that a calls b.

![cf](https://user-images.githubusercontent.com/64755588/168287506-b80a1e5c-68f0-4101-9b04-4e4b528920f4.png)

The next diagram shows the framework acting only with 4 threads which is the case where the multiThreadLevel argument is 4.
In this design all threads except thread 0 run three phases: 
Map, Sort and Reduce, while thread 0 also runs a special shuffle phase between its Sort and Reduce phases.
In the general case (where multiThreadLevel=n):

• Thread 0 runs Map, Sort, Shuffle and then Reduce.

• Threads 1 through n-1 runs Map, Sort and then Reduce.

The only thread that can run the shuffle phase is thread 0.

![de2](https://user-images.githubusercontent.com/64755588/168288451-d009b8ae-d014-4b0f-9d82-b897db2d83ee.png)
