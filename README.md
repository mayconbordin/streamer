streamer
========

Components:

- Stream: a communication channel that connects operators
- Operator: can be a source of tuples, a sink or a normal operator with input and
  output streams.
- Tuple: the unit of data to be processed by an operator and transported through a stream.
- Topology: the wiring of operators and streams, e.g. the DAG.



## WordCount Example

Stream sentences = builder.createStream("sentences", new Schema("sentence"));
Stream words     = builder.createStream("words", new Schema("word", "count"));
Stream counts    = builder.createStream("counts", new Schema("word", "count"));


builder.setSourceOperator("source", new SentenceSource(), 1);
builder.publish("source", sentences);

builder.setOperator("splitSentence", new SplitSentence(), 1);
builder.publish("splitSentence", words);
builder.subscribe("splitSentence", sentences);

builder.setOperator("wordCount", new WordCount(), 1);
builder.publish("wordCount", counts);
builder.subscribe("wordCount", words);

builder.build();


// subscription
builder.subscribe("wordCount", words, BROADCAST);
.subscribe("wordCount", words, SHUFFLE);
.subscribe("wordCount", words, GROUP_BY, "word");