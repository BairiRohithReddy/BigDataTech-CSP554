from mrjob.job import MRJob

class MRSalaries(MRJob):

    def mapper(self, _, line):
        (name,jobTitle,agencyID,agency,hireDate,annualSalary,grossPay) = line.split('\t')
        annualSalary = float(annualSalary)
        if annualSalary > 100000:
            salary_category = 'High'
        elif annualSalary >= 50000:
            salary_category = 'Medium'
        else:
            salary_category = 'Low'
        yield salary_category, 1

    def combiner(self, salary_category, counts):
        yield salary_category, sum(counts)

    def reducer(self, salary_category, counts):
        yield salary_category, sum(counts)

if __name__ == '__main__':
    MRSalaries.run()
