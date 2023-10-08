## Proposal for Electric Brew
An NEEFC Energy Efficiency Project with Austin Street Brewery

### Story

Austin Street Brewery, a rapidly growing craft beer brewery located in Maine, is actively looking to innovate in the realm of energy management. The brewery has expanded its operations and faces the challenge of managing energy consumption across seven different electricity meters. These meters are tied to various aspects of its operations, from the brewing process to customer engagement in the tasting room. Austin Street Brewery is committed to sustainability, sourcing 15% of their energy from community solar power. In collaboration with the New England Environmental Finance Center (NEEFC), the brewery aims to take data-driven steps to further optimize its energy efficiency, evaluate the ROI on community solar power, and set new benchmarks in the industry for sustainable practices.

Based on stakeholder feedback, we've revised our objectives to now focus on:
1. Operational cost mapping to understand which meters should power which operational facets.
2. Benchmarking against industry standards to identify areas for improvement.
3. Peak hour energy optimization to reduce costs.
4. Evaluating the ROI of various energy-saving initiatives as a stretch goal.

### Approach

To guide Austin Street Brewery toward energy efficiency and cost-effectiveness, we're employing a multi-stage analytical strategy. We'll begin with **data preprocessing and storage**, where we integrate and clean diverse datasets ranging from granular smart meter readings to utility bills and sustainability benchmarks. A deep dive into exploratory data analysis will follow, using visualizations to spotlight trends and correlations, such as how energy consumption varies with time or operational stages. Predictive models like regression will then help us pinpoint inefficiencies and suggest targeted optimizations for each of Austin Street's meters. At the same time, we'll measure Austin Street's energy metrics against industry benchmarks, employing statistical techniques like z-score normalization to guarantee apples-to-apples comparisons. Optimization algorithms will be designed for real-time energy adjustments during peak hours, and, as an ambitious stretch goal, we'll aim to formulate a financial model to assess the long-term returns on various energy-saving strategies.

Our approach is designed to be not just effective but also scalable and reproducible. One of our key objectives is to develop a framework that can easily be templated for other craft breweries, providing the New England Environmental Finance Center (NEEFC) with a potent tool for broader industry engagement. To that end, we plan to encapsulate our data preprocessing, analysis, and visualization steps in modular Python code. This modular design will ensure that our work is not only accessible and understandable for our immediate stakeholders but also sets a precedent for scalability and reuse in future NEEFC initiatives.

#### Risks
- **Granularity**: The CMP smart meter system provides data in 15-second increments, which could lead to enormous datasets. This high granularity might complicate data storage, processing, and timely analysis, potentially slowing down our project timeline.
  
- **Data Integration**: The brewery receives utility bills from multiple suppliers, each with its own billing structure. Reconciling these different data formats into a cohesive dataset for analysis could prove challenging, since their schemas are likely dissimilar.

- **Reporting Requirements**: The City of Portland has specific reporting requirements for energy consumption and sustainability metrics. Adhering to these standards may require additional data transformation steps, introducing another layer of complexity and time commitment to the project.

#### Unique Contributions
Our team brings a unique blend of expertise that spans data science, analytics, sustainability, and the brewing industry, making us exceptionally well-suited for this project. 

**Sean Sullivan**, who served as the Executive Director of the Maine Brewers' Guild for nearly a decade, has extensive experience in advising breweries on sustainability and energy management. His industry insights will be invaluable in translating data into actionable recommendations that align with brewery operations.

**Nelson Farrell** brings a strong mathematical background to the table, having previously developed linear regression models for [Project Gambit](https://github.com/jparkington/Project-Gambit), a chess analytics research project. His experience in creating predictive models will be crucial in fulfilling the project's deliverables related to energy optimization and cost savings.

**James Parkington**, with a decade of experience as a data and analytics engineer, has a track record of collecting and analyzing similar data for growing companies. His previous role at ButcherBox involved spearheading the data collection efforts that aided the company in becoming a certified B-Corp, showcasing his knowledge of sustainability metrics.

Together, we aim to build a comprehensive, accessible, and easily replicable analytical framework that can not only serve Austin Street Brewery but also set the groundwork for the NEEFC's future engagements with other breweries.
