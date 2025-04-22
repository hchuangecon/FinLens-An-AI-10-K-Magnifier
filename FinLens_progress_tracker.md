# FinLens Progress Tracker

## Project Overview
This pipeline extracts, processes and enables querying of SEC 10-K filings through a RAG-based approach.

## Development Status
```
FinLens/
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   ├── extraction/
│   │   ├── __init__.py
│   │   ├── sec_edgar_pipeline.py
│   │   └── scheduler.py
│   ├── parsing/
│   │   ├── __init__.py
│   │   └── document_parser.py
│   ├── qa/
│   │   ├── __init__.py
│   │   └── rag_pipeline.py
│   └── __init__.py
├── config/
│   └── default_config.json
├── tests/
│   ├── __init__.py
│   ├── test_extraction.py
│   ├── test_parsing.py
│   └── test_qa.py
├── .gitignore
├── README.md
├── requirements.txt
└── setup.py
```

### 1. Data Extraction Pipeline [⣿⣿⣿⣀⣀⣀⣀⣀⣀⣀] 25%
- [x] Research SEC EDGAR API authentication requirements
- [ ] Implement periodic filing scheduler
- [ ] Add 10-K document filtering
- [ ] Set up document storage

### 2. Data Parsing Pipeline [⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀] 0%
- [ ] Initialize Docling parser implementation 
- [ ] Define optimal chunking strategy
- [ ] Implement embedding generation
- [ ] Create structured data extractor
- [ ] Configure vector database storage
- [ ] Set up relational database schema

### 3. RAG QA Pipeline [⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀] 0%
- [ ] Build query classifier (textual vs. numerical)
- [ ] Create LLM query rewriter
- [ ] Implement context retrieval system
- [ ] Develop RAG response pipeline
- [ ] Build tabular data transformation system
- [ ] Create response formatter

## Technical Implementation Details

### Selected Components
- **Data Source**: SEC EDGAR API
- **Document Processing**: Docling
- **Storage**: To be determined (see comparisons below)
- **Query System**: To be determined (see comparisons below)

## Solution Comparison

### Vector Database Options

| Database | Pros | Cons | Best For |
|----------|------|------|----------|
| Pinecone | Managed service, fast queries, scaling | Higher cost | Production |
| Weaviate | Rich schema, multiple vector types | Complex setup | Semantic search |
| Qdrant | Open source, local deployment | More management | Cost-sensitive |
| Chroma | Simple API, easy integration | Less mature | Prototyping |

### LLM Options

| LLM | Pros | Cons | Best For |
|-----|------|------|----------|
| GPT-4 | Best financial comprehension | Expensive, rate limits | Accuracy-critical |
| Claude | Complex instruction handling | Higher costs | Nuanced queries |
| Llama 3 | Local deployment, lower cost | Less domain knowledge | Budget projects |

### RAG Framework Options

| Framework | Pros | Cons | Best For |
|-----------|------|------|----------|
| LangChain | Comprehensive, active community | Complex | Production |
| LlamaIndex | Document-focused, simpler API | Less modular | Document apps |
| Custom | Full control, specialized optimizations | Development overhead | Special needs |

## Recommendations
For optimal balance of performance, maintainability, and cost:

1. Data Source: SEC EDGAR API (established)
2. Storage: Pinecone (production) or Qdrant (budget-sensitive)
3. Relational DB: PostgreSQL with JSON support
4. LLM: GPT-4/Claude for accuracy, Llama 3 for cost savings
5. RAG Framework: LangChain for comprehensive tooling

## Next Action Items
1. Complete SEC EDGAR API authentication implementation
2. Evaluate storage options based on initial data volume estimates
3. Prototype basic chunking strategy with Docling