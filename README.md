# 1280. Students and Examinations

- ### **Problem Understanding**:
    We want to find out how many exams each student has attended for each subject. This involves:
    - Listing every possible combination of students and subjects.
    - Counting how many exams each student attended for each subject (if any).

- ### **General Approach** (Common for SQL, Spark, Pandas):
    1. **Creating All Possible Student-Subject Combinations**:
    - We need to ensure that every student is paired with every subject, regardless of whether the student attended any exam for that subject. This can be done using a **cross join** in SQL, Spark, and Pandas. 
            - **SQL**: You can use a `CROSS JOIN` to combine all students with all subjects.
            - **Spark/Pandas**: Use a merge with `how='cross'` to combine every student with every subject.

    2. **Counting Attended Exams**:
    - For each combination of student and subject, count how many times the student appears in the `examinations` table (i.e., how many exams the student attended for that subject).
            - **SQL**: Use `COUNT` to count the number of exams each student attended for each subject, with a `LEFT JOIN` to ensure all combinations (including those with no exam data) are included.
            - **Spark/Pandas**: Use `groupBy` (in Spark) or `groupby` (in Pandas) to group by `student_id` and `subject_name`, and then aggregate using `count` to get the number of attended exams.

    3. **Merging the Data**:
    - Combine the list of all student-subject pairs with the count of attended exams. For rows where no exam data exists for a student-subject pair, we'll use `0` to represent that the student didn’t attend any exam for that subject.
            - **SQL**: After counting the attended exams, perform a `LEFT JOIN` between the student-subject combinations and the counted exams data to bring in the exam count.
            - **Spark/Pandas**: Perform a `merge` (in Pandas) or `join` (in Spark) between the student-subject combinations and the attended exam count, ensuring missing values are replaced with `0`.

    4. **Handling Missing Values**:
        - In cases where no exams were attended by a student for a particular subject, the count of exams might be `NULL` or `NaN`. We need to replace these with `0` to indicate that the student didn't attend any exam.
            - **SQL**: Use the `COALESCE` function to replace `NULL` with `0`.
            - **Spark/Pandas**: Use `fillna(0)` to replace `NaN` values with `0`.

    5. **Final Output**:
        - The final output will be a table (or DataFrame) that lists each student, their subjects, and the number of exams they attended for each subject.

- ### **SQL Specifics**:
    - **CROSS JOIN**: To generate all combinations of students and subjects.
    - **LEFT JOIN**: To join the attended exams count with the student-subject pairs, ensuring we keep all student-subject pairs even if no exams were attended.
    - **GROUP BY and COUNT**: To count the number of exams attended for each student-subject pair.

- ### **Spark Specifics**:
    - **`join(..., how='cross')`**: To create the cross join of students and subjects.
    - **`groupBy(...).agg(count(...))`**: To group by student-subject pairs and count the attended exams.
    - **`fillna(0)`**: To replace `NaN` values with `0`.

- ### **Pandas Specifics**:
    - **`pd.merge(..., how='cross')`**: To perform a cross join.
    - **`groupby(...).size()`**: To count the number of exams attended by each student for each subject.
    - **`fillna(0)`**: To replace missing values with `0`.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT
            -- Select the student ID from the 'Students' table
            st.student_id, 
            
            -- Select the student name from the 'Students' table
            st.student_name, 
            
            -- Select the subject name from the 'Subjects' table
            sub.subject_name, 
            
            -- Count the number of exams attended by the student for the subject
            -- If no exam attended, return 0 using COALESCE to handle NULLs
            COALESCE(count(ex.student_id), 0) AS attended_exams
            
        FROM Students st 

        -- Perform a CROSS JOIN between 'Students' and 'Subjects'
        -- This creates all combinations of students and subjects
        CROSS JOIN 
        Subjects sub

        -- Perform a LEFT JOIN between the cross joined students and subjects
        -- with the 'Examinations' table to count the attended exams
        LEFT JOIN
        Examinations ex
        ON 
            -- Join condition: Match student IDs and subject names
            st.student_id = ex.student_id AND sub.subject_name = ex.subject_name
            
        -- Group the result by student ID, student name, and subject name
        GROUP BY st.student_id, st.student_name, sub.subject_name

        -- Order the results by student ID, student name, and subject name for easy readability
        ORDER BY st.student_id, st.student_name, sub.subject_name
        ```
    - **PySpark:**
        ```python3 []
        def students_and_examinations(students: pyspark.sql.dataframe.DataFrame, 
                                    subjects: pyspark.sql.dataframe.DataFrame, 
                                    examinations: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            
            # Perform a CROSS JOIN between 'students' and 'subjects'
            # This creates all combinations of students and subjects.
            cross_Join = students.join(subjects, how='cross')
            
            # Perform a LEFT JOIN between the result of the cross join and the 'examinations' DataFrame
            # This ensures that we keep all rows from the cross join, and we add examination data where available
            left_Join = cross_Join.join(examinations, on=['student_id', 'subject_name'], how='left')
            
            # Group the data by 'student_id', 'student_name', and 'subject_name' 
            # to count how many exams each student has attended
            # Use count to count the number of occurrences of student_id (which tells us the number of attended exams)
            # In case there are no exams for a student in a subject, the count will be 0
            students_With_Count_Subject_Attended_Exams = left_Join.groupBy(['student_id', 'student_name', 
                                                                            'subject_name'])\
                                                            .agg(count(examinations.student_id)
                                                                    .alias('attended_exams'))\
                                                            .orderBy(
                                                                # Order the grouped results by student_id,
                                                                # student_name, and subject_name for readability
                                                                ['student_id', 'student_name', 'subject_name'])\
                                                            .select(
                                                                    # Select the relevant columns: 
                                                                    # student_id, student_name, subject_name, 
                                                                    # and the count of attended exams
                                                                    ['student_id', 'student_name', 
                                                                    'subject_name', 'attended_exams'])
            
            # Return the resulting DataFrame containing students, subjects, and the count of attended exams
            return students_With_Count_Subject_Attended_Exams
        ```
    - **Pandas**
        ```python3 []
        def students_and_examinations(students: pd.DataFrame, 
                                    subjects: pd.DataFrame, 
                                    examinations: pd.DataFrame) -> pd.DataFrame:
            
            # Step 1: Group the 'examinations' DataFrame by 'student_id' and 'subject_name'
            # We count how many exams each student attended for each subject
            # This will give us the number of attended exams for each student-subject pair
            grouped = examinations.groupby(['student_id','subject_name']).size().reset_index(name = 'attended_exams')
            
            # Step 2: Create all possible combinations of students and subjects using a cross join
            # This combines every student with every subject, so we can later match them to the examinations data
            all_id_subjects = pd.merge(students, subjects, how = 'cross')
            
            # Step 3: Perform a LEFT JOIN to merge the all_id_subjects DataFrame with the grouped exams data
            # This step will add the attended exams count to the student-subject pairs
            # If a student did not attend any exams for a subject, the 'attended_exams' will be NaN
            students_With_Count_Subject_Attended_Exams = pd.merge(all_id_subjects, grouped, 
                                                                on = ['student_id', 'subject_name'], 
                                                                how = 'left')
            
            # Step 4: Replace all NaN values (which mean no exams attended) with 0
            # This means that if a student has no exam data for a subject, we'll show 0 instead of NaN
            students_With_Count_Subject_Attended_Exams.fillna(0, inplace = True)
            
            # Step 5: Return the final DataFrame, which includes the student, subject, and attended exam count
            return students_With_Count_Subject_Attended_Exams
        ```