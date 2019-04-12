#---------- IF ------------#
my_age = input(" Input your age : ")

# Karena input(my_age) dari user berupa string maka harus diubah dulu menjadi integer sebelum dibandingkan pada IF dengan cara berikut :
age = int(my_age)

if(age<10):
    print(" You are children ")
elif(age<20):
    print(" You are teenager")
else:
    print(" You are old")
print(my_age)

#---------- Loop ------------#
# While
count = 0
while(count<5):
    print(" count : ",count)
    count += 1

# For
array = ["Teknologi","Informasi","Teknik","Udayana"]
# print(array[1])
for i in array:
    print(i)

#---------- List ------------#
list = ["Teknologi",11,"Informasi",2018,"Teknik"] # List dapat terdiri dari tipe data yang berbeda

list[2]=1991 # Replace value dari list index ke-2

del(list[0])  # Delete list index ke-0

list.append("Udayana") # Menambahkan isi list di akhir list

list.append("Start") # Menambahkan isi list di depan list

print(list)