def twoSum(nums, target):
    solution = []
    for i in range(len(nums)):
        for j in range(i + 1, (len(nums))):
            if nums[i] + nums[j] == target:
                print(nums[i] + nums[j])
                solution.append([i,j])
    return solution

print(twoSum([2,7,11,15,10,-1],9))