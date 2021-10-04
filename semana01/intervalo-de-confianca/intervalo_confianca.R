###########################################################
####### Intervalo de Confiança #######
## AED - Capitulo 03 - Prof. Máiron Chaves ####
###########################################################

#Copie este código, cole no seu R e execute para ver os resultados
########## Intervalo de confiança para média amostral pela distribuição Normal Padrão #############################
# Obter o intervalo de confiança para uma variável cuja média = 30, desvio padrão =7,31 e n = 30
#Temos que definir o nível de confiança do nosso intervalo.
#Podemos obter o valor do quantil para o nível de confiança desejado com a função
qnorm()

#O quantil na distribuição normal padrão para 95% de confiança
ic <- 0.95
alfa <- 1-ic
1-(alfa/2)
qnorm(0.975)
#Vamos armazenar os valores em objetos
media <- 30
desvio_padrao_populacional <- 7.31
n <- 30
quantil_95 <- qnorm(0.975)
#Aplicando a fórmula vista na apostila fica:
Limite_Superior <- 30+quantil_95*(desvio_padrao_populacional/sqrt(n))
Limite_Inferior <- 30-quantil_95*(desvio_padrao_populacional/sqrt(n))
paste("Com 95% de confiança, podemos afirmar que a média varia
entre",Limite_Inferior," e ",Limite_Superior)


