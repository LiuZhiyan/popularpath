## Quick start

Besides of the source code there is an out-of-box build in the package for quick check, which includes two example access log files and executable jar program. You can launch ``run_cases.sh`` located at project root in your bash directly, it will start twice program to process these two access log files orderly, you can check the result in standard output of the shell.

> **Note**: To help delivery the package build to your environment correctly and efficiently, including co-work with automatic service orchestration tool, I'd like to prepare a Dockerfile in the code trunk if/when it is needed.

## Code structure

- Package ``io.lzy.popular_path.model`` contains all ``Graph`` implementation related classes.
	* **Graph**: Base Graph implementation, which contains all generic functions.
		* **GraphRandom**: A Graph implementation which supports to generate graph base on random node access and allow client ad-hoc query popular path contains any number of sequential nodes.
		* **GraphSequence**: A Graph implementation which supports to generate graph base on sequence node access. It requires client provides the number of sequential nodes of the path when creating graph. So the dynamics about popular path query of GraphRandom is better than this implementation however this graph provides much better query performance especially when client query more then once.
	* **Node**: Node object which organizes the graph.
	* **Edge**: Edge object which link each nodes together in the graph.
- package ``io.lzy.popular_path`` contains all access log process logic related classes.
	* **LogParser**: To parse the node access log from an input stream and load user and page into the graph.
	* **PopularPath**:  As the entry point of the program, parse input arguments and call ``LogParser`` and ``Graph`` functions.

> **Note**:
> - The program is used to process sample access log for performance or function test only, the input arguments ``PopularPath`` current supported are very limited.  The complete and more powerful interfaces were not leveraged in ``PopularPath``, to use these functions as a library from your real program are encouraged.
> - Exploring the source code and inline java doc comments are highly recommended.
