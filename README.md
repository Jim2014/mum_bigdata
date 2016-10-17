# Bigdata Course Project - Crystal Ball
*This project is for course project of MUM BigData*
## How to run this project?
1. Download the [Cloudra QuickStart VM(for CDH5.8)](http://www.cloudera.com/downloads/quickstart_vms/5-8.html)
  * You can select the VM image for VMWare,VirtualBox or KVM, I used the VMWare image.
  * Make sure that 4+GiB RAM for the VM
2. Develop Environment
  * Eclipse Luna (included in theVM)
3. Create a java project and copy the java class files to the src folder
4. Add external jars to the build path of your project 
  * File /usr/lib/haddoop/client-0.20
  * File /usr/lib/hadoop
  * File /usr/lib/hadoop/lib
5. Every java class file provide a solution
  * Include inner class Mapper
  * Include inner class Reducer
  * Other inner class needed
  * Each java class file include a Main method, you can run the Main method and see the rusult.
  
## Java class files
1. CrystalBallPair.java
  * This is for Part2
  * Implement pairs algorithm to compute relative frequencies
2. CrystalBallStripe.java
  * This is for Part3
  * Implement stripes algorithm to compute relative frequencies
3. CrystalBallPairStripe.java
  * This is for Part4
  * Implement pairs in mapper and stripes in reducer to compute relative frequencies (hybrid approach)

## Input file - input.txt 
  ```
  Mary 34 56 29 12 34 56 92 29 34 12
  Kelly 92 29 12 34 79 29 56 12 34 18
  ```
    
   
