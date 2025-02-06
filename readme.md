## **How to Run the Project**

1. **Install the Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Update the Configuration:**

   Edit `config.yaml` with your AWS and PostgreSQL credentials and adjust file paths as needed.

3. **Run the ETL Pipeline:**

   ```bash
   python main.py
   ```

4. **Run the API (Optional):**

   ```bash
   uvicorn api:app --reload
   ```

---

### Explanation

- **CFP Data Handling:**  
  The CFP data provides the fractions (0 to 1) for various energy sources (renewable, nonrenewable_coal, nonrenewable_gas, nonrenewable_nuclear, nonrenewable_oil, etc.).

- **Data Ingestion & Processing:**  
  The energy data is read from an Excel file (with multiple sheets) and resampled to 15-minute intervals, with missing values handled using interpolation if the gap is less than or equal to 2 hours.

- **Database & API:**  
  The processed data is loaded into PostgreSQL, and the API (using FastAPI) provides endpoints to query monthly carbon emissions.

This complete and refactored project now fully integrates the details provided about the ENTSO‑E CFP data and is ready for further testing and deployment. Enjoy coding!